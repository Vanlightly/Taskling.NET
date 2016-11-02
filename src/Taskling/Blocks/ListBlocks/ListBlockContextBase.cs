using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Retries;
using Taskling.Serialization;

namespace Taskling.Blocks.ListBlocks
{
    public class ListBlockContextBase<TItem, THeader>
    {
        #region .: Protected Fields :.

        protected IListBlockRepository _listBlockRepository;
        protected ITaskExecutionRepository _taskExecutionRepository;
        protected string _applicationName;
        protected string _taskName;
        protected string _taskExecutionId;
        protected int _maxStatusReasonLength;
        protected object _uncommittedListSyncRoot = new object();
        protected object _getItemsSyncRoot = new object();
        protected List<IListBlockItem<TItem>> _uncommittedItems;
        protected bool _completed;

        private bool _hasHeader;

        #endregion .: Protected Fields :.


        #region .: Constructor :.

        public ListBlockContextBase(IListBlockRepository listBlockRepository,
            ITaskExecutionRepository taskExecutionRepository,
            string applicationName,
            string taskName,
            string taskExecutionId,
            ListUpdateMode listUpdateMode,
            int uncommittedThreshold,
            ListBlock<TItem> listBlock,
            string blockExecutionId,
            int maxStatusReasonLength,
            string forcedBlockQueueId = "0")
        {
            _listBlockRepository = listBlockRepository;
            _taskExecutionRepository = taskExecutionRepository;
            _headerlessBlock = listBlock;
            BlockExecutionId = blockExecutionId;
            ListUpdateMode = listUpdateMode;
            ForcedBlockQueueId = forcedBlockQueueId;
            UncommittedThreshold = uncommittedThreshold;
            _applicationName = applicationName;
            _taskName = taskName;
            _taskExecutionId = taskExecutionId;
            _maxStatusReasonLength = maxStatusReasonLength;

            if (listUpdateMode != ListUpdateMode.SingleItemCommit)
                _uncommittedItems = new List<IListBlockItem<TItem>>();

            _completed = false;
        }

        public ListBlockContextBase(IListBlockRepository listBlockRepository,
            ITaskExecutionRepository taskExecutionRepository,
            string applicationName,
            string taskName,
            string taskExecutionId,
            ListUpdateMode listUpdateMode,
            int uncommittedThreshold,
            ListBlock<TItem, THeader> listBlock,
            string blockExecutionId,
            int maxStatusReasonLength,
            string forcedBlockQueueId = "0")
        {
            _listBlockRepository = listBlockRepository;
            _taskExecutionRepository = taskExecutionRepository;
            _blockWithHeader = listBlock;
            BlockExecutionId = blockExecutionId;
            ListUpdateMode = listUpdateMode;
            ForcedBlockQueueId = forcedBlockQueueId;
            UncommittedThreshold = uncommittedThreshold;
            _applicationName = applicationName;
            _taskName = taskName;
            _taskExecutionId = taskExecutionId;
            _maxStatusReasonLength = maxStatusReasonLength;

            if (listUpdateMode != ListUpdateMode.SingleItemCommit)
                _uncommittedItems = new List<IListBlockItem<TItem>>();

            _completed = false;
            _hasHeader = true;
        }

        #endregion .: Constructor :.


        #region .: Protected Properties :.

        protected string BlockExecutionId { get; set; }
        protected ListUpdateMode ListUpdateMode { get; set; }
        protected int UncommittedThreshold { get; set; }

        #endregion .: Protected Properties :.


        #region .: Public Propeties :.

        public string ListBlockId
        {
            get
            {
                if (_hasHeader)
                    return _blockWithHeader.ListBlockId;

                return _headerlessBlock.ListBlockId;
            }
        }

        protected ListBlock<TItem> _headerlessBlock;

        protected ListBlock<TItem, THeader> _blockWithHeader;

        public string ForcedBlockQueueId { get; private set; }

        #endregion .: Public Propeties :.


        #region .: Protected Methods :.

        protected bool disposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
            {
                CommitUncommittedItems();
            }

            disposed = true;
        }

        protected void ValidateBlockIsActive()
        {
            if (_completed)
                throw new ExecutionException("The block has been marked as completed");
        }

        protected void SetStatusAsFailed()
        {
            var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
                    _taskExecutionId,
                    BlockType.List,
                    BlockExecutionId,
                    BlockExecutionStatus.Failed);

            Action<BlockExecutionChangeStatusRequest> actionRequest = _listBlockRepository.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        protected void UpdateItemStatus(IListBlockItem<TItem> item)
        {
            switch (ListUpdateMode)
            {
                case ListUpdateMode.SingleItemCommit:
                    Commit(ListBlockId, item);
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

        protected void Commit(string listBlockId, IListBlockItem<TItem> item)
        {
            var singleUpdateRequest = new SingleUpdateRequest()
            {
                TaskId = new TaskId(_applicationName, _taskName),
                ListBlockId = listBlockId,
                ListBlockItem = Convert(item)
            };

            Action<SingleUpdateRequest> actionRequest = _listBlockRepository.UpdateListBlockItem;
            RetryService.InvokeWithRetry(actionRequest, singleUpdateRequest);
        }

        protected void AddToUncommittedItems(IListBlockItem<TItem> item)
        {
            lock (_uncommittedListSyncRoot)
            {
                _uncommittedItems.Add(item);
            }
        }

        protected void CommitIfUncommittedCountReached()
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

        protected void CommitUncommittedItems()
        {
            List<IListBlockItem<TItem>> listToCommit = null;
            lock (_uncommittedListSyncRoot)
            {
                if (_uncommittedItems != null && _uncommittedItems.Any())
                {
                    listToCommit = new List<IListBlockItem<TItem>>(_uncommittedItems);
                    _uncommittedItems.Clear();
                }
            }

            if (listToCommit != null && listToCommit.Any())
            {
                var batchUpdateRequest = new BatchUpdateRequest()
                {
                    TaskId = new TaskId(_applicationName, _taskName),
                    ListBlockId = ListBlockId,
                    ListBlockItems = Convert(listToCommit)
                };

                Action<BatchUpdateRequest> actionRequest = _listBlockRepository.BatchUpdateListBlockItems;
                RetryService.InvokeWithRetry(actionRequest, batchUpdateRequest);
            }
        }

        protected List<ProtoListBlockItem> Convert(List<IListBlockItem<TItem>> listBlockItems)
        {
            var items = new List<ProtoListBlockItem>();

            foreach (var listBlockItem in listBlockItems)
                items.Add(Convert(listBlockItem));

            return items;
        }

        protected ProtoListBlockItem Convert(IListBlockItem<TItem> listBlockItem)
        {
            return new ProtoListBlockItem()
            {
                LastUpdated = listBlockItem.LastUpdated,
                ListBlockItemId = listBlockItem.ListBlockItemId,
                Status = listBlockItem.Status,
                StatusReason = LimitLength(listBlockItem.StatusReason, _maxStatusReasonLength),
                Step = listBlockItem.Step
            };
        }

        protected List<IListBlockItem<TItem>> Convert(IList<ProtoListBlockItem> listBlockItems)
        {
            var items = new List<IListBlockItem<TItem>>();

            foreach (var listBlockItem in listBlockItems)
                items.Add(Convert(listBlockItem));

            return items;
        }

        protected IListBlockItem<TItem> Convert(ProtoListBlockItem listBlockItem)
        {
            return new ListBlockItem<TItem>()
            {
                LastUpdated = listBlockItem.LastUpdated,
                ListBlockItemId = listBlockItem.ListBlockItemId,
                Status = listBlockItem.Status,
                StatusReason = listBlockItem.StatusReason,
                Step = listBlockItem.Step,
                Value = JsonGenericSerializer.Deserialize<TItem>(listBlockItem.Value)
            };
        }

        protected string LimitLength(string input, int limit)
        {
            if (input == null)
                return null;

            if (limit < 1)
                return input;

            if (input.Length > limit)
                return input.Substring(0, limit);

            return input;
        }

        private IList<IListBlockItem<TItem>> GetItems()
        {
            if (_hasHeader)
                return _blockWithHeader.Items;

            return _headerlessBlock.Items;
        }

        private void SetItems(IList<IListBlockItem<TItem>> items)
        {
            if (_hasHeader)
                _blockWithHeader.Items = items;

            _headerlessBlock.Items = items;
        }

        private IEnumerable<IListBlockItem<TItem>> GetItemsFromHeaderlessBlock(params ItemStatus[] statuses)
        {
            if (statuses.Length == 0)
                statuses = new[] { ItemStatus.All };

            lock (_getItemsSyncRoot)
            {
                if (_headerlessBlock.Items == null || !_headerlessBlock.Items.Any())
                {
                    var protoListBlockItems = _listBlockRepository.GetListBlockItems(new TaskId(_applicationName, _taskName), ListBlockId);
                    _headerlessBlock.Items = Convert(protoListBlockItems);

                    foreach (var item in _headerlessBlock.Items)
                    {
                        ((ListBlockItem<TItem>)item).SetParentContext(this.ItemComplete, this.ItemFailed, this.DiscardItem);
                    }
                }

                if (statuses.Any(x => x == ItemStatus.All))
                    return _headerlessBlock.Items.Where(x => x.Status == ItemStatus.Failed || x.Status == ItemStatus.Pending || x.Status == ItemStatus.Discarded || x.Status == ItemStatus.Completed).ToList();

                return _headerlessBlock.Items.Where(x => statuses.Contains(x.Status)).ToList();
            }
        }

        private IEnumerable<IListBlockItem<TItem>> GetItemsFromBlockWithHeader(params ItemStatus[] statuses)
        {
            if (statuses.Length == 0)
                statuses = new[] { ItemStatus.All };

            lock (_getItemsSyncRoot)
            {
                if (_blockWithHeader.Items == null || !_blockWithHeader.Items.Any())
                {
                    var protoListBlockItems = _listBlockRepository.GetListBlockItems(new TaskId(_applicationName, _taskName), ListBlockId);
                    _blockWithHeader.Items = Convert(protoListBlockItems);

                    foreach (var item in _blockWithHeader.Items)
                    {
                        ((ListBlockItem<TItem>)item).SetParentContext(this.ItemComplete, this.ItemFailed, this.DiscardItem);
                    }
                }

                if (statuses.Any(x => x == ItemStatus.All))
                    return _blockWithHeader.Items.Where(x => x.Status == ItemStatus.Failed || x.Status == ItemStatus.Pending || x.Status == ItemStatus.Discarded || x.Status == ItemStatus.Completed).ToList();

                return _blockWithHeader.Items.Where(x => statuses.Contains(x.Status)).ToList();
            }
        }

        #endregion .: Private/Protected Methods :.


        #region .: Public Methods :.

        public void FillItems()
        {
            lock (_getItemsSyncRoot)
            {
                var protoListBlockItems = _listBlockRepository.GetListBlockItems(new TaskId(_applicationName, _taskName), ListBlockId);
                var listBlockItems = Convert(protoListBlockItems);
                SetItems(listBlockItems);

                foreach (var item in listBlockItems)
                {
                    ((ListBlockItem<TItem>)item).SetParentContext(this.ItemComplete, this.ItemFailed, this.DiscardItem);
                }
            }
        }

        public IEnumerable<IListBlockItem<TItem>> GetItems(params ItemStatus[] statuses)
        {
            if (_hasHeader)
                return GetItemsFromBlockWithHeader(statuses);

            return GetItemsFromHeaderlessBlock(statuses);
        }

        public void ItemComplete(IListBlockItem<TItem> item)
        {
            ValidateBlockIsActive();
            item.Status = ItemStatus.Completed;
            UpdateItemStatus(item);
        }

        public void ItemFailed(IListBlockItem<TItem> item, string reason, byte? step = null)
        {
            item.StatusReason = reason;

            if (step.HasValue)
                item.Step = step;

            ValidateBlockIsActive();
            item.Status = ItemStatus.Failed;
            UpdateItemStatus(item);
        }

        public void DiscardItem(IListBlockItem<TItem> item, string reason, byte? step = null)
        {
            item.StatusReason = reason;
            if (step.HasValue)
                item.Step = step;

            ValidateBlockIsActive();
            item.Status = ItemStatus.Discarded;
            UpdateItemStatus(item);
        }

        public void Start()
        {
            ValidateBlockIsActive();
            var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
               _taskExecutionId,
               BlockType.List,
               BlockExecutionId,
               BlockExecutionStatus.Started);

            Action<BlockExecutionChangeStatusRequest> actionRequest = _listBlockRepository.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        public void Complete()
        {
            ValidateBlockIsActive();
            CommitUncommittedItems();

            var status = BlockExecutionStatus.Completed;
            if (GetItems(ItemStatus.Failed, ItemStatus.Pending).Any())
                status = BlockExecutionStatus.Failed;

            var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
               _taskExecutionId,
               BlockType.List,
               BlockExecutionId,
               status);

            Action<BlockExecutionChangeStatusRequest> actionRequest = _listBlockRepository.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        public void Failed()
        {
            ValidateBlockIsActive();
            CommitUncommittedItems();
            SetStatusAsFailed();
        }

        public void Failed(string message)
        {
            Failed();

            string errorMessage = string.Format("BlockId {0} Error: {1}", ListBlockId, message);
            var errorRequest = new TaskExecutionErrorRequest()
            {
                TaskId = new TaskId(_applicationName, _taskName),
                TaskExecutionId = _taskExecutionId,
                TreatTaskAsFailed = false,
                Error = errorMessage
            };
            _taskExecutionRepository.Error(errorRequest);
        }

        public IEnumerable<TItem> GetItemValues(params ItemStatus[] statuses)
        {
            if (statuses.Length == 0)
                statuses = new[] { ItemStatus.All };

            return GetItems(statuses).Select(x => x.Value);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion .: Public Methods :.
    }
}
