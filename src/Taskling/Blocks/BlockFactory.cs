using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Contexts;
using Taskling.Blocks.Requests;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.Blocks
{
    public class BlockFactory : IBlockFactory
    {
        private readonly IBlockService _blockService;
        private readonly IRangeBlockService _rangeBlockService;
        private readonly IListBlockService _listBlockService;

        public BlockFactory(IBlockService blockService, IRangeBlockService rangeBlockService, IListBlockService listBlockService)
        {
            _blockService = blockService;
            _rangeBlockService = rangeBlockService;
            _listBlockService = listBlockService;
        }

        public IList<IRangeBlockContext> GenerateDateRangeBlocks(DateRangeBlockRequest blockRequest)
        {
            var blocks = new List<IRangeBlockContext>();
            int blocksRemaining = LoadFailedAndDeadBlocks(blockRequest, blocks);

            if (blocksRemaining > 0)
                blocks.AddRange(GenerateNewDateRangeBlocks(blockRequest, blocksRemaining));

            return blocks;
        }

        public IList<IRangeBlockContext> GenerateNumericRangeBlocks(NumericRangeBlockRequest blockRequest)
        {
            var blocks = new List<IRangeBlockContext>();
            int blocksRemaining = LoadFailedAndDeadBlocks(blockRequest, blocks);

            if (blocksRemaining > 0)
                blocks.AddRange(GenerateNewNumericRangeBlocks(blockRequest, blocksRemaining));

            return blocks;
        }

        public IList<IListBlockContext> GenerateListBlocks(ListBlockRequest blockRequest)
        {
            var blocks = new List<IListBlockContext>();
            int blocksRemaining = LoadFailedAndDeadListBlocks(blockRequest, blocks);

            if (blocksRemaining > 0)
                blocks.AddRange(GenerateNewListBlocks(blockRequest, blocksRemaining));

            return blocks;
        }

        #region .: Range Blocks :.

        private int LoadFailedAndDeadBlocks(BlockRequest blockRequest, List<IRangeBlockContext> blocks)
        {
            int blocksRemaining = blockRequest.MaxBlocks;
            if (blockRequest.CheckForDeadExecutions)
            {
                blocks.AddRange(GetDeadBlocks(blockRequest, blocksRemaining));
                blocksRemaining = blockRequest.MaxBlocks - blocks.Count;
            }

            if (blocksRemaining > 0 && blockRequest.CheckForFailedExecutions)
            {
                blocks.AddRange(GetFailedBlocks(blockRequest, blocksRemaining));
                blocksRemaining = blockRequest.MaxBlocks - blocks.Count;
            }

            return blocksRemaining;
        }

        private List<IRangeBlockContext> GetDeadBlocks(BlockRequest blockRequest, int blockCountLimit)
        {
            var deadBlockRequest = CreateDeadBlocksRequest(blockRequest, blockCountLimit);
            var deadBlocks = _blockService.FindDeadRangeBlocks(deadBlockRequest);
            return CreateBlockContexts(blockRequest, deadBlocks);
        }
        
        private List<IRangeBlockContext> GetFailedBlocks(BlockRequest blockRequest, int blockCountLimit)
        {
            var failedBlockRequest = new FindFailedBlocksRequest(blockRequest.ApplicationName,
                blockRequest.TaskName,
                blockRequest.TaskExecutionId,
                blockRequest.BlockType,
                DateTime.UtcNow - blockRequest.GoBackTimePeriodForFailedTasks,
                blockCountLimit
            );

            var failedBlocks = _blockService.FindFailedRangeBlocks(failedBlockRequest);
            return CreateBlockContexts(blockRequest, failedBlocks);
        }

        private List<IRangeBlockContext> CreateBlockContexts(BlockRequest blockRequest, IList<RangeBlock> rangeBlocks)
        {
            var blocks = new List<IRangeBlockContext>();
            foreach (var rangeBlock in rangeBlocks)
            {
                var blockContext = CreateBlockContext(blockRequest, rangeBlock);
                blocks.Add(blockContext);
            }

            return blocks;
        }

        private IRangeBlockContext CreateBlockContext(BlockRequest blockRequest, RangeBlock rangeBlock)
        {
            var createRequest = new BlockExecutionCreateRequest(blockRequest.ApplicationName,
                    blockRequest.TaskName,
                    blockRequest.TaskExecutionId,
                    blockRequest.BlockType,
                    rangeBlock.RangeBlockId);

            var blockExecutionId = _blockService.AddRangeBlockExecution(createRequest);
            var blockContext = new RangeBlockContext(_rangeBlockService,
                    blockRequest.ApplicationName,
                    blockRequest.TaskName,
                    blockRequest.TaskExecutionId, 
                    rangeBlock, blockExecutionId);

            return blockContext;
        }

        private List<IRangeBlockContext> GenerateNewDateRangeBlocks(DateRangeBlockRequest blockRequest, int blockCountLimit)
        {
            var newBlocks = new List<IRangeBlockContext>();
            DateTime blockStart = blockRequest.RangeBegin;
            DateTime blockEnd = blockStart.Add(blockRequest.MaxBlockRange);
            int blocksAdded = 0;

            while (blocksAdded < blockCountLimit && blockStart < blockRequest.RangeEnd)
            {
                if (blockEnd > blockRequest.RangeEnd)
                    blockEnd = blockRequest.RangeEnd;

                var dateRangeBlock = GenerateDateRangeBlock(blockRequest, blockStart, blockEnd);
                var blockContext = CreateBlockContext(blockRequest, dateRangeBlock);
                newBlocks.Add(blockContext);
                blocksAdded++;

                blockStart = blockStart.Add(blockRequest.MaxBlockRange);
                blockEnd = blockStart.Add(blockRequest.MaxBlockRange);

            }

            return newBlocks;
        }

        private RangeBlock GenerateDateRangeBlock(DateRangeBlockRequest blockRequest, DateTime rangeBegin, DateTime rangeEnd)
        {
            var request = new RangeBlockCreateRequest(blockRequest.ApplicationName,
                blockRequest.TaskName,
                blockRequest.TaskExecutionId,
                rangeBegin,
                rangeEnd);

            return _blockService.AddRangeBlock(request).Block;
        }

        private List<IRangeBlockContext> GenerateNewNumericRangeBlocks(NumericRangeBlockRequest blockRequest, int blockCountLimit)
        {
            var newBlocks = new List<IRangeBlockContext>();
            long blockStart = blockRequest.RangeBegin;
            long blockEnd = blockStart + blockRequest.BlockSize;
            int blocksAdded = 0;

            while (blocksAdded < blockCountLimit && blockStart < blockRequest.RangeEnd)
            {
                if (blockEnd > blockRequest.RangeEnd)
                    blockEnd = blockRequest.RangeEnd;

                var numericRangeBlock = GenerateNumericRangeBlock(blockRequest, blockStart, blockEnd);
                var blockContext = CreateBlockContext(blockRequest, numericRangeBlock);
                newBlocks.Add(blockContext);
                blocksAdded++;

                blockStart = blockStart + blockRequest.BlockSize;
                blockEnd = blockStart + blockRequest.BlockSize;
            }

            return newBlocks;
        }

        private RangeBlock GenerateNumericRangeBlock(NumericRangeBlockRequest blockRequest, long rangeBegin, long rangeEnd)
        {
            var request = new RangeBlockCreateRequest(blockRequest.ApplicationName,
                blockRequest.TaskName,
                blockRequest.TaskExecutionId,
                rangeBegin,
                rangeEnd);

            return _blockService.AddRangeBlock(request).Block;
        }

        #endregion .: Range Blocks :.

        #region .: List Blocks :.

        private int LoadFailedAndDeadListBlocks(ListBlockRequest blockRequest, List<IListBlockContext> blocks)
        {
            int blocksRemaining = blockRequest.MaxBlocks;
            if (blockRequest.CheckForDeadExecutions)
            {
                blocks.AddRange(GetDeadListBlocks(blockRequest, blocksRemaining));
                blocksRemaining = blockRequest.MaxBlocks - blocks.Count;
            }

            if (blocksRemaining > 0 && blockRequest.CheckForFailedExecutions)
            {
                blocks.AddRange(GetFailedListBlocks(blockRequest, blocksRemaining));
                blocksRemaining = blockRequest.MaxBlocks - blocks.Count;
            }

            return blocksRemaining;
        }

        private List<IListBlockContext> GetDeadListBlocks(ListBlockRequest blockRequest, int blockCountLimit)
        {
            var deadBlockRequest = CreateDeadBlocksRequest(blockRequest, blockCountLimit);
            var deadBlocks = _blockService.FindDeadListBlocks(deadBlockRequest);
            return CreateListBlockContexts(blockRequest, deadBlocks);
        }

        private List<IListBlockContext> GetFailedListBlocks(ListBlockRequest blockRequest, int blockCountLimit)
        {
            var failedBlockRequest = new FindFailedBlocksRequest(blockRequest.ApplicationName,
                blockRequest.TaskName,
                blockRequest.TaskExecutionId,
                blockRequest.BlockType,
                DateTime.UtcNow-blockRequest.GoBackTimePeriodForFailedTasks,
                blockCountLimit
            );

            var failedBlocks = _blockService.FindFailedListBlocks(failedBlockRequest);
            return CreateListBlockContexts(blockRequest, failedBlocks);
        }

        private List<IListBlockContext> CreateListBlockContexts(ListBlockRequest blockRequest, IList<ListBlock> listBlocks)
        {
            var blocks = new List<IListBlockContext>();
            foreach (var listBlock in listBlocks)
            {
                var blockContext = CreateListBlockContext(blockRequest, listBlock);
                blocks.Add(blockContext);
            }

            return blocks;
        }

        private IListBlockContext CreateListBlockContext(ListBlockRequest blockRequest, ListBlock listBlock)
        {
            var createRequest = new BlockExecutionCreateRequest(blockRequest.ApplicationName,
                    blockRequest.TaskName,
                    blockRequest.TaskExecutionId,
                    blockRequest.BlockType,
                    listBlock.ListBlockId);

            var blockExecutionId = _blockService.AddListBlockExecution(createRequest);
            var blockContext = new ListBlockContext(_listBlockService,
                    blockRequest.ApplicationName,
                    blockRequest.TaskName,
                    blockRequest.TaskExecutionId,
                    blockRequest.ListUpdateMode,
                    blockRequest.UncommittedItemsThreshold,
                    listBlock, blockExecutionId);

            return blockContext;
        }

        private List<IListBlockContext> GenerateNewListBlocks(ListBlockRequest blockRequest, int blockCountLimit)
        {
            var newBlocks = new List<IListBlockContext>();
            int listLength = blockRequest.Values.Count;
            int listIndex = 0;
            int blocksAdded = 0;

            var values = new List<string>() ;
            
            while (blocksAdded < blockCountLimit && listIndex < listLength)
            {
                values.Add(blockRequest.Values[listIndex]);

                if (values.Count == blockRequest.MaxBlockSize || listIndex == listLength-1)
                {
                    var newListBlock = GenerateListBlock(blockRequest, values);
                    var blockContext = CreateListBlockContext(blockRequest, newListBlock);
                    newBlocks.Add(blockContext);
                    values = new List<string>();
                    blocksAdded++;
                }

                listIndex++;
            }

            return newBlocks;
        }

        private ListBlock GenerateListBlock(ListBlockRequest blockRequest, List<string> values)
        {
            var request = new ListBlockCreateRequest(blockRequest.ApplicationName,
                blockRequest.TaskName,
                blockRequest.TaskExecutionId,
                values);

            return _blockService.AddListBlock(request).Block;
        }

        #endregion .: List Blocks :.

        #region .: Common Logic :.

        private FindDeadBlocksRequest CreateDeadBlocksRequest(BlockRequest blockRequest, int blockCountLimit)
        {
            var utcNow = DateTime.UtcNow;

            if (blockRequest.TaskDeathMode == TaskDeathMode.Override)
            {
                return new FindDeadBlocksRequest(blockRequest.ApplicationName,
                    blockRequest.TaskName,
                    blockRequest.TaskExecutionId,
                    blockRequest.BlockType,
                    utcNow - blockRequest.OverrideDeathThreshold,
                    utcNow-blockRequest.GoBackTimePeriodForDeadTasks,
                    blockCountLimit
                    );
            }

            if (blockRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                return new FindDeadBlocksRequest(blockRequest.ApplicationName,
                    blockRequest.TaskName,
                    blockRequest.TaskExecutionId,
                    blockRequest.BlockType,
                    utcNow-blockRequest.KeepAliveDeathThreshold,
                    blockCountLimit
                    );
            }

            throw new NotSupportedException("TaskDeathMode not supported");
        }

        #endregion .: Common Logic :.
    }
}
