using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.Retries;

namespace Taskling.Blocks.Contexts
{
    public class ListBlockContext : IListBlockContext, IDisposable
    {
        private readonly IListBlockService _listBlockService;
        private readonly string _applicationName;
        private readonly string _taskName;
        private readonly string _taskExecutionId;
        private object _uncommittedListSyncRoot = new object();
        private readonly List<ListBlockItem> _uncommittedItems;
        
        public ListBlockContext(IListBlockService listBlockService,
            string applicationName,
            string taskName,
            string taskExecutionId,
            ListUpdateMode listUpdateMode,
            int uncommittedThreshold,
            ListBlock listBlock, 
            string blockExecutionId)
        {
            _listBlockService = listBlockService;
            Block = listBlock;
            BlockExecutionId = blockExecutionId;
            ListUpdateMode = listUpdateMode;
            UncommittedThreshold = uncommittedThreshold;
            _applicationName = applicationName;
            _taskName = taskName;
            _taskExecutionId = taskExecutionId;

            if (listUpdateMode != ListUpdateMode.SingleItemCommit)
                _uncommittedItems = new List<ListBlockItem>();
        }

        private ListBlock Block { get; set; }
        private string BlockExecutionId { get; set; }
        private ListUpdateMode ListUpdateMode { get; set; }
        private int UncommittedThreshold { get; set; }

        public string ListBlockId
        {
            get
            {
                return Block.ListBlockId;
            }
        }

        public IEnumerable<ListBlockItem> GetAllItems()
        {
            if (Block.Items == null || !Block.Items.Any())
                Block.Items = _listBlockService.GetListBlockItems(Block.ListBlockId).ToList();

            return Block.Items;
        }

        public IEnumerable<ListBlockItem> GetFailedAndPendingItems()
        {
            if (Block.Items == null || !Block.Items.Any())
            {
                Block.Items = _listBlockService.GetListBlockItems(Block.ListBlockId)
                    .Where(x => x.Status == ListBlockItemStatus.Failed || x.Status == ListBlockItemStatus.Pending)
                    .ToList();

                return Block.Items;
            }

            return Block.Items.Where(x => x.Status == ListBlockItemStatus.Failed || x.Status == ListBlockItemStatus.Pending).ToList();
        }

        public void ItemComplete(ListBlockItem item)
        {
            item.Status = ListBlockItemStatus.Completed;
            UpdateItemStatus(item);
        }

        public void ItemFailed(ListBlockItem item)
        {
            item.Status = ListBlockItemStatus.Failed;
            UpdateItemStatus(item);
        }

        public void Start()
        {
            var request = new BlockExecutionChangeStatusRequest(_applicationName,
               _taskName,
               _taskExecutionId,
               BlockType.List,
               BlockExecutionId,
               BlockExecutionStatus.Started);

            Action<BlockExecutionChangeStatusRequest> actionRequest = _listBlockService.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        public void Complete()
        {
            CommitUncommittedItems();

            var request = new BlockExecutionChangeStatusRequest(_applicationName,
               _taskName,
               _taskExecutionId,
               BlockType.List,
               BlockExecutionId,
               BlockExecutionStatus.Completed);

            Action<BlockExecutionChangeStatusRequest> actionRequest = _listBlockService.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        public void Failed()
        {
            CommitUncommittedItems();

            var request = new BlockExecutionChangeStatusRequest(_applicationName,
               _taskName,
               _taskExecutionId,
               BlockType.List,
               BlockExecutionId,
               BlockExecutionStatus.Failed);

            Action<BlockExecutionChangeStatusRequest> actionRequest = _listBlockService.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        public void Dispose()
        {
            CommitUncommittedItems();
        }

        private void UpdateItemStatus(ListBlockItem item)
        {
            switch (ListUpdateMode)
            {
                case ListUpdateMode.SingleItemCommit:
                    Commit(Block.ListBlockId, item);
                    break;
                case ListUpdateMode.BatchCommitAtEnd:
                    AddToUncommittedItems(item);
                    break;
                case ListUpdateMode.PeriodicBatchCommit:
                    AddToUncommittedItems(item);
                    CommitIfUncommittedCountReached();
                    break;
            }
        }

        private void Commit(string listBlockId, ListBlockItem item)
        {
            var singleUpdateRequest = new SingleUpdateRequest()
            {
                ListBlockId = listBlockId,
                ListBlockItem = item
            };

            Action<SingleUpdateRequest> actionRequest = _listBlockService.UpdateListBlockItem;
            RetryService.InvokeWithRetry(actionRequest, singleUpdateRequest);
        }

        private void AddToUncommittedItems(ListBlockItem item)
        {
            lock (_uncommittedListSyncRoot)
            {
                _uncommittedItems.Add(item);
            }
        }

        private void CommitIfUncommittedCountReached()
        {
            bool shouldCommit = false;
            lock (_uncommittedListSyncRoot)
            {
                if (_uncommittedItems.Count == UncommittedThreshold)
                    shouldCommit = true;
            }

            if (shouldCommit)
                CommitUncommittedItems();
        }

        private void CommitUncommittedItems()
        {
            List<ListBlockItem> listToCommit = null;
            lock (_uncommittedListSyncRoot)
            {
                if (_uncommittedItems != null && _uncommittedItems.Any())
                {
                    listToCommit = new List<ListBlockItem>(_uncommittedItems);
                    _uncommittedItems.Clear();
                }
            }

            if (listToCommit != null && listToCommit.Any())
            {
                var batchUpdateRequest = new BatchUpdateRequest()
                {
                    ListBlockId = Block.ListBlockId,
                    ListBlockItems = listToCommit
                };

                Action<BatchUpdateRequest> actionRequest = _listBlockService.BatchUpdateListBlockItems;
                RetryService.InvokeWithRetry(actionRequest, batchUpdateRequest);
            }
        }
    }
}
