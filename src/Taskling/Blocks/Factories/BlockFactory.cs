using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.Blocks.Requests;
using Taskling.Contexts;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.Blocks.ObjectBlocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Serialization;

namespace Taskling.Blocks.Factories
{
    public class BlockFactory : IBlockFactory
    {
        private readonly IBlockRepository _blockRepository;
        private readonly IRangeBlockRepository _rangeBlockRepository;
        private readonly IListBlockRepository _listBlockRepository;
        private readonly IObjectBlockRepository _objectBlockRepository;
        private readonly ITaskExecutionRepository _taskExecutionRepository;

        public BlockFactory(IBlockRepository blockRepository,
            IRangeBlockRepository rangeBlockRepository,
            IListBlockRepository listBlockRepository,
            IObjectBlockRepository objectBlockRepository,
            ITaskExecutionRepository taskExecutionRepository)
        {
            _blockRepository = blockRepository;
            _rangeBlockRepository = rangeBlockRepository;
            _listBlockRepository = listBlockRepository;
            _objectBlockRepository = objectBlockRepository;
            _taskExecutionRepository = taskExecutionRepository;
        }

        public IList<IDateRangeBlockContext> GenerateDateRangeBlocks(DateRangeBlockRequest blockRequest)
        {
            var blocks = new List<RangeBlockContext>();

            if (!string.IsNullOrEmpty(blockRequest.ReprocessReferenceValue))
            {
                blocks = LoadRangeBlocksOfTask(blockRequest);
            }
            else
            {
                var forceBlocks = GetForcedBlocks(blockRequest);
                blocks.AddRange(forceBlocks);

                if (GetBlocksRemaining(blockRequest, blocks) > 0)
                    LoadFailedAndDeadBlocks(blockRequest, blocks);

                int blocksRemaining = GetBlocksRemaining(blockRequest, blocks);
                if (blocksRemaining > 0 && blockRequest.RangeBegin.HasValue)
                    blocks.AddRange(GenerateNewDateRangeBlocks(blockRequest, blocksRemaining));
            }

            if (!blocks.Any())
            {
                LogEmptyBlockEvent(blockRequest.TaskExecutionId, blockRequest.ApplicationName, blockRequest.TaskName);
            }

            var dateRangeBlocks = blocks.Select(x => (IDateRangeBlockContext)x);
            return dateRangeBlocks.OrderBy(x => long.Parse(x.DateRangeBlock.RangeBlockId)).ToList();
        }

        public IList<INumericRangeBlockContext> GenerateNumericRangeBlocks(NumericRangeBlockRequest blockRequest)
        {
            var blocks = new List<RangeBlockContext>();

            if (!string.IsNullOrEmpty(blockRequest.ReprocessReferenceValue))
            {
                blocks = LoadRangeBlocksOfTask(blockRequest);
            }
            else
            {
                var forceBlocks = GetForcedBlocks(blockRequest);
                blocks.AddRange(forceBlocks);

                if (GetBlocksRemaining(blockRequest, blocks) > 0)
                    LoadFailedAndDeadBlocks(blockRequest, blocks);

                int blocksRemaining = GetBlocksRemaining(blockRequest, blocks);
                if (blocksRemaining > 0 && blockRequest.RangeBegin.HasValue)
                    blocks.AddRange(GenerateNewNumericRangeBlocks(blockRequest, blocksRemaining));
            }

            if (!blocks.Any())
            {
                LogEmptyBlockEvent(blockRequest.TaskExecutionId, blockRequest.ApplicationName, blockRequest.TaskName);
            }

            var numericRangeBlocks = blocks.Select(x => (INumericRangeBlockContext)x);
            return numericRangeBlocks.OrderBy(x => long.Parse(x.NumericRangeBlock.RangeBlockId)).ToList();
        }

        public IList<IListBlockContext<T>> GenerateListBlocks<T>(ListBlockRequest blockRequest)
        {
            var blocks = CreateProtoListBlocks(blockRequest);
            var blockContexts = CreateListBlockContexts<T>(blockRequest, blocks).ToList();

            if (blocks.Any(x => x.IsForcedBlock))
                DequeueForcedBlocks(blockRequest, blocks.Where(x => x.IsForcedBlock).Select(x => x.ForcedBlockQueueId.ToString()).ToList());

            if (!blocks.Any())
            {
                LogEmptyBlockEvent(blockRequest.TaskExecutionId, blockRequest.ApplicationName, blockRequest.TaskName);
            }

            return blockContexts.OrderBy(x => long.Parse(x.Block.ListBlockId)).ToList();
        }

        public IList<IListBlockContext<TItem, THeader>> GenerateListBlocks<TItem, THeader>(ListBlockRequest blockRequest)
        {
            var blocks = CreateProtoListBlocks(blockRequest);
            var blockContexts = CreateListBlockContexts<TItem, THeader>(blockRequest, blocks).ToList();

            if (blocks.Any(x => x.IsForcedBlock))
                DequeueForcedBlocks(blockRequest, blocks.Where(x => x.IsForcedBlock).Select(x => x.ForcedBlockQueueId.ToString()).ToList());

            if (!blocks.Any())
            {
                LogEmptyBlockEvent(blockRequest.TaskExecutionId, blockRequest.ApplicationName, blockRequest.TaskName);
            }

            return blockContexts.OrderBy(x => long.Parse(x.Block.ListBlockId)).ToList();
        }

        public IList<IObjectBlockContext<T>> GenerateObjectBlocks<T>(ObjectBlockRequest<T> blockRequest)
        {
            var blocks = new List<IObjectBlockContext<T>>();

            if (!string.IsNullOrEmpty(blockRequest.ReprocessReferenceValue))
            {
                blocks = LoadObjectBlocksOfTask(blockRequest);
            }
            else
            {
                var forceBlocks = GetForcedObjectBlocks(blockRequest);
                blocks.AddRange(forceBlocks);

                if (GetBlocksRemaining(blockRequest, blocks) > 0)
                    LoadFailedAndDeadObjectBlocks(blockRequest, blocks);

                if (GetBlocksRemaining(blockRequest, blocks) > 0 && blockRequest.Object != null)
                {
                    var newBlock = GenerateNewObjectBlock(blockRequest);
                    blocks.Add(newBlock);
                }
            }

            if (!blocks.Any())
            {
                LogEmptyBlockEvent(blockRequest.TaskExecutionId, blockRequest.ApplicationName, blockRequest.TaskName);
            }

            return blocks.OrderBy(x => long.Parse(x.Block.ObjectBlockId)).ToList();
        }

        public IListBlock<T> GetLastListBlock<T>(LastBlockRequest lastBlockRequest)
        {
            var lastProtoListBlock = _listBlockRepository.GetLastListBlock(lastBlockRequest);
            if (lastProtoListBlock == null)
                return null;

            return Convert<T>(lastProtoListBlock, true);
        }

        public IListBlock<TItem, THeader> GetLastListBlock<TItem, THeader>(LastBlockRequest lastBlockRequest)
        {
            var lastProtoListBlock = _listBlockRepository.GetLastListBlock(lastBlockRequest);

            return Convert<TItem, THeader>(lastProtoListBlock, true);
        }


        #region .: Range Blocks :.

        private void LogEmptyBlockEvent(string taskExecutionId, string appName, string taskName)
        {
            var checkPointRequest = new TaskExecutionCheckpointRequest()
            {
                TaskExecutionId = taskExecutionId,
                Message = "No values for generate the block. Emtpy Block context returned.",
                TaskId = new TaskId(appName, taskName)
            };
            _taskExecutionRepository.Checkpoint(checkPointRequest);
        }

        private int GetBlocksRemaining(BlockRequest blockRequest, List<RangeBlockContext> blocks)
        {
            return blockRequest.MaxBlocks - blocks.Count;
        }

        private List<RangeBlockContext> GetForcedBlocks(BlockRequest blockRequest)
        {
            var forcedBlockRequest = new QueuedForcedBlocksRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                    blockRequest.TaskExecutionId,
                    blockRequest.BlockType);

            var queuedForcedBlocks = _blockRepository.GetQueuedForcedRangeBlocks(forcedBlockRequest);

            var forcedBlocks = new List<RangeBlockContext>();
            foreach (var queuedForcedBlock in queuedForcedBlocks)
                forcedBlocks.Add(CreateBlockContext(blockRequest, queuedForcedBlock.RangeBlock, queuedForcedBlock.ForcedBlockQueueId));

            if (forcedBlocks.Any())
                DequeueForcedBlocks(blockRequest, forcedBlocks.Select(x => x.ForcedBlockQueueId).ToList());

            return forcedBlocks;
        }

        private void DequeueForcedBlocks(BlockRequest blockRequest, List<string> forcedBlockQueueIds)
        {
            var request = new DequeueForcedBlocksRequest(
                new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                blockRequest.TaskExecutionId,
                blockRequest.BlockType,
                forcedBlockQueueIds);

            _blockRepository.DequeueForcedBlocks(request);
        }

        private List<RangeBlockContext> LoadRangeBlocksOfTask(BlockRequest blockRequest)
        {
            var failedBlockRequest = new FindBlocksOfTaskRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                blockRequest.TaskExecutionId,
                blockRequest.BlockType,
                blockRequest.ReprocessReferenceValue,
                blockRequest.ReprocessOption);

            var blocksOfTask = _blockRepository.FindRangeBlocksOfTask(failedBlockRequest).ToList();
            if (!blocksOfTask.Any())
                return new List<RangeBlockContext>();

            return CreateBlockContexts(blockRequest, blocksOfTask);
        }

        private IList<ProtoListBlock> LoadListBlocksOfTask(ListBlockRequest blockRequest)
        {
            var failedBlockRequest = new FindBlocksOfTaskRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                blockRequest.TaskExecutionId,
                blockRequest.BlockType,
                blockRequest.ReprocessReferenceValue,
                blockRequest.ReprocessOption);

            var blocksOfTask = _blockRepository.FindListBlocksOfTask(failedBlockRequest);

            return blocksOfTask;
        }

        private void LoadFailedAndDeadBlocks(BlockRequest blockRequest, List<RangeBlockContext> blocks)
        {
            int blocksRemaining = blockRequest.MaxBlocks - blocks.Count;
            if (blockRequest.ReprocessDeadTasks)
            {
                blocks.AddRange(GetDeadBlocks(blockRequest, blocksRemaining));
            }

            if (GetBlocksRemaining(blockRequest, blocks) > 0 && blockRequest.ReprocessFailedTasks)
            {
                blocks.AddRange(GetFailedBlocks(blockRequest, blocksRemaining));
            }
        }

        private List<RangeBlockContext> GetDeadBlocks(BlockRequest blockRequest, int blockCountLimit)
        {
            var deadBlockRequest = CreateDeadBlocksRequest(blockRequest, blockCountLimit);
            var deadBlocks = _blockRepository.FindDeadRangeBlocks(deadBlockRequest);
            return CreateBlockContexts(blockRequest, deadBlocks);
        }

        private List<RangeBlockContext> GetFailedBlocks(BlockRequest blockRequest, int blockCountLimit)
        {
            var failedBlockRequest = new FindFailedBlocksRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                blockRequest.TaskExecutionId,
                blockRequest.BlockType,
                DateTime.UtcNow - blockRequest.FailedTaskDetectionRange,
                DateTime.UtcNow,
                blockCountLimit,
                blockRequest.FailedTaskRetryLimit
            );

            var failedBlocks = _blockRepository.FindFailedRangeBlocks(failedBlockRequest);
            return CreateBlockContexts(blockRequest, failedBlocks);
        }

        private List<RangeBlockContext> CreateBlockContexts(BlockRequest blockRequest, IList<RangeBlock> rangeBlocks)
        {
            var blocks = new List<RangeBlockContext>();
            foreach (var rangeBlock in rangeBlocks)
            {
                var blockContext = CreateBlockContext(blockRequest, rangeBlock);
                blocks.Add(blockContext);
            }

            return blocks;
        }

        private RangeBlockContext CreateBlockContext(BlockRequest blockRequest,
            RangeBlock rangeBlock,
            int forcedBlockQueueId = 0)
        {
            var attempt = rangeBlock.Attempt + 1;
            var createRequest = new BlockExecutionCreateRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                    blockRequest.TaskExecutionId,
                    blockRequest.BlockType,
                    rangeBlock.RangeBlockId,
                    attempt);

            var blockExecutionId = _blockRepository.AddRangeBlockExecution(createRequest);
            var blockContext = new RangeBlockContext(_rangeBlockRepository,
                    _taskExecutionRepository,
                    blockRequest.ApplicationName,
                    blockRequest.TaskName,
                    blockRequest.TaskExecutionId,
                    rangeBlock, blockExecutionId,
                    forcedBlockQueueId.ToString());

            return blockContext;
        }

        private List<RangeBlockContext> GenerateNewDateRangeBlocks(DateRangeBlockRequest blockRequest, int blockCountLimit)
        {
            var newBlocks = new List<RangeBlockContext>();
            DateTime blockStart = blockRequest.RangeBegin.Value;
            DateTime blockEnd = blockStart.Add(blockRequest.MaxBlockRange.Value);
            int blocksAdded = 0;
            bool stopGeneration = false;

            while (blocksAdded < blockCountLimit && blockStart <= blockRequest.RangeEnd && stopGeneration == false)
            {
                if (blockEnd >= blockRequest.RangeEnd.Value)
                {
                    blockEnd = blockRequest.RangeEnd.Value;
                    stopGeneration = true;
                }

                var dateRangeBlock = GenerateDateRangeBlock(blockRequest, blockStart, blockEnd);
                var blockContext = CreateBlockContext(blockRequest, dateRangeBlock);
                newBlocks.Add(blockContext);
                blocksAdded++;

                blockStart = blockStart.Add(blockRequest.MaxBlockRange.Value);
                blockEnd = blockStart.Add(blockRequest.MaxBlockRange.Value);

            }

            return newBlocks;
        }

        private RangeBlock GenerateDateRangeBlock(DateRangeBlockRequest blockRequest, DateTime rangeBegin, DateTime rangeEnd)
        {
            var request = new RangeBlockCreateRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                blockRequest.TaskExecutionId,
                rangeBegin,
                rangeEnd);

            var rangeBlock = _blockRepository.AddRangeBlock(request).Block;
            Thread.Sleep(5); // guarantee that each block has a unique created date
            return rangeBlock;
        }

        private List<RangeBlockContext> GenerateNewNumericRangeBlocks(NumericRangeBlockRequest blockRequest, int blockCountLimit)
        {
            var newBlocks = new List<RangeBlockContext>();
            long blockStart = blockRequest.RangeBegin.Value;
            long blockEnd = blockStart + (blockRequest.BlockSize.Value - 1);
            int blocksAdded = 0;
            bool stopGeneration = false;

            while (blocksAdded < blockCountLimit && blockStart <= blockRequest.RangeEnd.Value && stopGeneration == false)
            {
                if (blockEnd >= blockRequest.RangeEnd.Value)
                {
                    blockEnd = blockRequest.RangeEnd.Value;
                    stopGeneration = true;
                }

                var numericRangeBlock = GenerateNumericRangeBlock(blockRequest, blockStart, blockEnd);
                var blockContext = CreateBlockContext(blockRequest, numericRangeBlock);
                newBlocks.Add(blockContext);
                blocksAdded++;

                blockStart = blockStart + blockRequest.BlockSize.Value;
                blockEnd = blockStart + (blockRequest.BlockSize.Value - 1);
            }

            return newBlocks;
        }

        private RangeBlock GenerateNumericRangeBlock(NumericRangeBlockRequest blockRequest, long rangeBegin, long rangeEnd)
        {
            var request = new RangeBlockCreateRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                blockRequest.TaskExecutionId,
                rangeBegin,
                rangeEnd);

            var rangeBlock = _blockRepository.AddRangeBlock(request).Block;
            Thread.Sleep(5); // guarantee that each block has a unique created date
            return rangeBlock;
        }

        #endregion .: Range Blocks :.

        #region .: List Blocks :.

        #region .: Create ProtoListBlocks :.

        private List<ProtoListBlock> CreateProtoListBlocks(ListBlockRequest blockRequest)
        {
            var blocks = new List<ProtoListBlock>();

            if (!string.IsNullOrEmpty(blockRequest.ReprocessReferenceValue))
            {
                blocks = LoadListBlocksOfTask(blockRequest).ToList();
            }
            else
            {
                // Forced blocks
                var forceBlockQueueItems = GetForcedListBlocks(blockRequest);
                var forceBlocks = new List<ProtoListBlock>();
                foreach (var forceBlockQueueItem in forceBlockQueueItems)
                {
                    var forceBlock = forceBlockQueueItem.ListBlock;
                    forceBlock.IsForcedBlock = true;
                    forceBlock.ForcedBlockQueueId = forceBlockQueueItem.ForcedBlockQueueId;
                    forceBlocks.Add(forceBlock);
                }

                blocks.AddRange(forceBlocks);

                // Failed and Dead blocks
                if (GetBlocksRemaining(blockRequest, blocks) > 0)
                    LoadFailedAndDeadListBlocks(blockRequest, blocks);

                // New blocks
                int blocksRemaining = GetBlocksRemaining(blockRequest, blocks);
                if (blocksRemaining > 0 && blockRequest.SerializedValues != null && blockRequest.SerializedValues.Any())
                {
                    blocks.AddRange(GenerateNewListBlocks(blockRequest, blocksRemaining));
                }
            }

            return blocks;
        }

        private int GetBlocksRemaining(ListBlockRequest blockRequest, List<ProtoListBlock> blocks)
        {
            return blockRequest.MaxBlocks - blocks.Count;
        }

        private IList<ForcedListBlockQueueItem> GetForcedListBlocks(ListBlockRequest blockRequest)
        {
            var forcedBlockRequest = new QueuedForcedBlocksRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                    blockRequest.TaskExecutionId,
                    blockRequest.BlockType);

            var queuedForcedBlocks = _blockRepository.GetQueuedForcedListBlocks(forcedBlockRequest);

            return queuedForcedBlocks;
        }

        private void LoadFailedAndDeadListBlocks(ListBlockRequest blockRequest, List<ProtoListBlock> blocks)
        {
            int blocksRemaining = GetBlocksRemaining(blockRequest, blocks);
            if (blockRequest.ReprocessDeadTasks)
            {
                blocks.AddRange(GetDeadListBlocks(blockRequest, blocksRemaining));
                blocksRemaining = blockRequest.MaxBlocks - blocks.Count;
            }

            if (GetBlocksRemaining(blockRequest, blocks) > 0 && blockRequest.ReprocessFailedTasks)
            {
                blocks.AddRange(GetFailedListBlocks(blockRequest, blocksRemaining));
            }
        }

        private IList<ProtoListBlock> GetDeadListBlocks(ListBlockRequest blockRequest, int blockCountLimit)
        {
            var deadBlockRequest = CreateDeadBlocksRequest(blockRequest, blockCountLimit);
            var deadBlocks = _blockRepository.FindDeadListBlocks(deadBlockRequest);

            return deadBlocks;
        }

        private IList<ProtoListBlock> GetFailedListBlocks(ListBlockRequest blockRequest, int blockCountLimit)
        {
            var failedBlockRequest = new FindFailedBlocksRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                blockRequest.TaskExecutionId,
                blockRequest.BlockType,
                DateTime.UtcNow - blockRequest.FailedTaskDetectionRange,
                DateTime.UtcNow,
                blockCountLimit,
                blockRequest.FailedTaskRetryLimit
            );

            var failedBlocks = _blockRepository.FindFailedListBlocks(failedBlockRequest);
            return failedBlocks;
        }

        private List<ProtoListBlock> GenerateNewListBlocks(ListBlockRequest blockRequest, int blockCountLimit)
        {
            var newBlocks = new List<ProtoListBlock>();
            int listLength = blockRequest.SerializedValues.Count;
            int listIndex = 0;
            int blocksAdded = 0;

            var values = new List<string>();

            while (blocksAdded < blockCountLimit && listIndex < listLength)
            {
                values.Add(blockRequest.SerializedValues[listIndex]);

                if (values.Count == blockRequest.MaxBlockSize || listIndex == listLength - 1)
                {
                    var newListBlock = GenerateListBlock(blockRequest, values);
                    newBlocks.Add(newListBlock);
                    values = new List<string>();
                    blocksAdded++;
                }

                listIndex++;
            }

            return newBlocks;
        }

        private ProtoListBlock GenerateListBlock(ListBlockRequest blockRequest, List<string> values)
        {
            var request = new ListBlockCreateRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                blockRequest.TaskExecutionId,
                values,
                blockRequest.SerializedHeader,
                blockRequest.CompressionThreshold);

            var listBlock = _blockRepository.AddListBlock(request).Block;
            Thread.Sleep(5); // guarantee that each block has a unique created date
            return listBlock;
        }

        #endregion .: Create ProtoListBlocks :.

        #region .: Create List Contexts :.

        private IList<IListBlockContext<T>> CreateListBlockContexts<T>(ListBlockRequest blockRequest, IList<ProtoListBlock> listBlocks)
        {
            var blocks = new List<IListBlockContext<T>>();
            foreach (var listBlock in listBlocks)
            {
                var blockContext = CreateListBlockContext<T>(blockRequest, listBlock);
                blocks.Add(blockContext);
            }

            return blocks;
        }

        private IListBlockContext<T> CreateListBlockContext<T>(ListBlockRequest blockRequest, ProtoListBlock listBlock, int forcedBlockQueueId = 0)
        {
            var attempt = listBlock.Attempt + 1;
            var createRequest = new BlockExecutionCreateRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                    blockRequest.TaskExecutionId,
                    BlockType.List,
                    listBlock.ListBlockId,
                    attempt);

            var blockExecutionId = _blockRepository.AddListBlockExecution(createRequest);

            var listBlockOfT = Convert<T>(listBlock);
            var blockContext = new ListBlockContext<T>(_listBlockRepository,
                    _taskExecutionRepository,
                    blockRequest.ApplicationName,
                    blockRequest.TaskName,
                    blockRequest.TaskExecutionId,
                    blockRequest.ListUpdateMode,
                    blockRequest.UncommittedItemsThreshold,
                    listBlockOfT,
                    blockExecutionId,
                    blockRequest.MaxStatusReasonLength,
                    forcedBlockQueueId.ToString());

            return blockContext;
        }

        private IList<IListBlockContext<TItem, THeader>> CreateListBlockContexts<TItem, THeader>(ListBlockRequest blockRequest, IList<ProtoListBlock> listBlocks)
        {
            var blocks = new List<IListBlockContext<TItem, THeader>>();
            foreach (var listBlock in listBlocks)
            {
                var blockContext = CreateListBlockContext<TItem, THeader>(blockRequest, listBlock);
                blocks.Add(blockContext);
            }

            return blocks;
        }

        private IListBlockContext<TItem, THeader> CreateListBlockContext<TItem, THeader>(ListBlockRequest blockRequest, ProtoListBlock listBlock, int forcedBlockQueueId = 0)
        {
            var attempt = listBlock.Attempt + 1;
            var createRequest = new BlockExecutionCreateRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                    blockRequest.TaskExecutionId,
                    blockRequest.BlockType,
                    listBlock.ListBlockId,
                    attempt);

            var blockExecutionId = _blockRepository.AddListBlockExecution(createRequest);

            var listBlockOfT = Convert<TItem, THeader>(listBlock);
            var blockContext = new ListBlockContext<TItem, THeader>(_listBlockRepository,
                    _taskExecutionRepository,
                    blockRequest.ApplicationName,
                    blockRequest.TaskName,
                    blockRequest.TaskExecutionId,
                    blockRequest.ListUpdateMode,
                    blockRequest.UncommittedItemsThreshold,
                    listBlockOfT,
                    blockExecutionId,
                    blockRequest.MaxStatusReasonLength,
                    forcedBlockQueueId.ToString());

            return blockContext;
        }

        private ListBlock<T> Convert<T>(ProtoListBlock protoListBlock, bool fillBlock = false)
        {
            if (protoListBlock == null)
                return null;

            var block = new ListBlock<T>();
            block.Attempt = protoListBlock.Attempt;

            if (fillBlock)
                block.Items = Convert<T>(protoListBlock.Items);

            block.ListBlockId = protoListBlock.ListBlockId;

            return block;
        }

        private ListBlock<TItem, THeader> Convert<TItem, THeader>(ProtoListBlock protoListBlock, bool fillBlock = false)
        {
            if (protoListBlock == null)
                return null;

            var block = new ListBlock<TItem, THeader>();
            block.Attempt = protoListBlock.Attempt;

            if (fillBlock)
                block.Items = Convert<TItem>(protoListBlock.Items);

            block.ListBlockId = protoListBlock.ListBlockId;
            block.Header = JsonGenericSerializer.Deserialize<THeader>(protoListBlock.Header);

            return block;
        }

        private IList<IListBlockItem<T>> Convert<T>(IList<ProtoListBlockItem> protoListBlockItems)
        {
            if (protoListBlockItems == null)
                return null;

            var items = new List<IListBlockItem<T>>();

            foreach (var protoItem in protoListBlockItems)
            {
                var item = new ListBlockItem<T>();
                item.LastUpdated = protoItem.LastUpdated;
                item.ListBlockItemId = protoItem.ListBlockItemId;
                item.Status = protoItem.Status;
                item.StatusReason = protoItem.StatusReason;
                item.Step = protoItem.Step;
                item.Value = JsonGenericSerializer.Deserialize<T>(protoItem.Value);

                items.Add(item);
            }

            return items;
        }

        #endregion .: Create List Contexts :.

        #endregion .: List Blocks :.

        #region .: Object Blocks :.

        private List<IObjectBlockContext<T>> LoadObjectBlocksOfTask<T>(ObjectBlockRequest<T> blockRequest)
        {
            var failedBlockRequest = new FindBlocksOfTaskRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                blockRequest.TaskExecutionId,
                blockRequest.BlockType,
                blockRequest.ReprocessReferenceValue,
                blockRequest.ReprocessOption);

            var blocksOfTask = _blockRepository.FindObjectBlocksOfTask<T>(failedBlockRequest);
            if (!blocksOfTask.Any())
                return new List<IObjectBlockContext<T>>();

            return CreateObjectBlockContexts(blockRequest, blocksOfTask);
        }

        private List<IObjectBlockContext<T>> GetForcedObjectBlocks<T>(ObjectBlockRequest<T> blockRequest)
        {
            var forcedBlockRequest = new QueuedForcedBlocksRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                    blockRequest.TaskExecutionId,
                    blockRequest.BlockType);

            var queuedForcedBlocks = _blockRepository.GetQueuedForcedObjectBlocks<T>(forcedBlockRequest);

            var forcedBlocks = new List<IObjectBlockContext<T>>();
            foreach (var queuedForcedBlock in queuedForcedBlocks)
                forcedBlocks.Add(CreateObjectBlockContext(blockRequest, queuedForcedBlock.ObjectBlock, queuedForcedBlock.ForcedBlockQueueId));

            if (forcedBlocks.Any())
                DequeueForcedBlocks(blockRequest, forcedBlocks.Select(x => x.ForcedBlockQueueId).ToList());

            return forcedBlocks;
        }

        private void LoadFailedAndDeadObjectBlocks<T>(ObjectBlockRequest<T> blockRequest, List<IObjectBlockContext<T>> blocks)
        {
            int blocksRemaining = GetBlocksRemaining(blockRequest, blocks);
            if (blockRequest.ReprocessDeadTasks)
            {
                blocks.AddRange(GetDeadObjectBlocks(blockRequest, blocksRemaining));
                blocksRemaining = blockRequest.MaxBlocks - blocks.Count;
            }

            if (GetBlocksRemaining(blockRequest, blocks) > 0 && blockRequest.ReprocessFailedTasks)
            {
                blocks.AddRange(GetFailedObjectBlocks(blockRequest, blocksRemaining));
            }
        }

        private List<IObjectBlockContext<T>> GetDeadObjectBlocks<T>(ObjectBlockRequest<T> blockRequest, int blockCountLimit)
        {
            var deadBlockRequest = CreateDeadBlocksRequest(blockRequest, blockCountLimit);
            var deadBlocks = _blockRepository.FindDeadObjectBlocks<T>(deadBlockRequest);
            return CreateObjectBlockContexts(blockRequest, deadBlocks);
        }

        private List<IObjectBlockContext<T>> GetFailedObjectBlocks<T>(ObjectBlockRequest<T> blockRequest, int blockCountLimit)
        {
            var failedBlockRequest = new FindFailedBlocksRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                blockRequest.TaskExecutionId,
                blockRequest.BlockType,
                DateTime.UtcNow - blockRequest.FailedTaskDetectionRange,
                DateTime.UtcNow,
                blockCountLimit,
                blockRequest.FailedTaskRetryLimit
            );

            var failedBlocks = _blockRepository.FindFailedObjectBlocks<T>(failedBlockRequest);
            return CreateObjectBlockContexts<T>(blockRequest, failedBlocks);
        }

        private List<IObjectBlockContext<T>> CreateObjectBlockContexts<T>(ObjectBlockRequest<T> blockRequest, IList<ObjectBlock<T>> objectBlocks)
        {
            var blocks = new List<IObjectBlockContext<T>>();
            foreach (var objectBlock in objectBlocks)
            {
                var blockContext = CreateObjectBlockContext(blockRequest, objectBlock);
                blocks.Add(blockContext);
            }

            return blocks;
        }

        private IObjectBlockContext<T> CreateObjectBlockContext<T>(ObjectBlockRequest<T> blockRequest, ObjectBlock<T> objectBlock, int forcedBlockQueueId = 0)
        {
            var attempt = objectBlock.Attempt + 1;
            var createRequest = new BlockExecutionCreateRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                    blockRequest.TaskExecutionId,
                    blockRequest.BlockType,
                    objectBlock.ObjectBlockId,
                    attempt);

            var blockExecutionId = _blockRepository.AddObjectBlockExecution(createRequest);
            var blockContext = new ObjectBlockContext<T>(_objectBlockRepository,
                    _taskExecutionRepository,
                    blockRequest.ApplicationName,
                    blockRequest.TaskName,
                    blockRequest.TaskExecutionId,
                    objectBlock,
                    blockExecutionId,
                    forcedBlockQueueId.ToString());

            return blockContext;
        }

        private IObjectBlockContext<T> GenerateNewObjectBlock<T>(ObjectBlockRequest<T> blockRequest)
        {
            var newObjectBlock = GenerateObjectBlock(blockRequest);
            return CreateObjectBlockContext(blockRequest, newObjectBlock);
        }

        private ObjectBlock<T> GenerateObjectBlock<T>(ObjectBlockRequest<T> blockRequest)
        {
            var request = new ObjectBlockCreateRequest<T>(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                blockRequest.TaskExecutionId,
                blockRequest.Object,
                blockRequest.CompressionThreshold);

            var objectBlock = _blockRepository.AddObjectBlock(request).Block;
            Thread.Sleep(5); // guarantee that each block has a unique created date
            return objectBlock;
        }

        private int GetBlocksRemaining<T>(BlockRequest blockRequest, List<IObjectBlockContext<T>> blocks)
        {
            return blockRequest.MaxBlocks - blocks.Count;
        }

        #endregion .: Object Blocks :.

        #region .: Common Logic :.

        private FindDeadBlocksRequest CreateDeadBlocksRequest(BlockRequest blockRequest, int blockCountLimit)
        {
            var utcNow = DateTime.UtcNow;

            return new FindDeadBlocksRequest(new TaskId(blockRequest.ApplicationName, blockRequest.TaskName),
                    blockRequest.TaskExecutionId,
                    blockRequest.BlockType,
                    utcNow - blockRequest.DeadTaskDetectionRange,
                    utcNow.AddMinutes(-1),
                    blockCountLimit,
                    blockRequest.TaskDeathMode,
                    blockRequest.DeadTaskRetryLimit);
        }

        #endregion .: Common Logic :.
    }
}
