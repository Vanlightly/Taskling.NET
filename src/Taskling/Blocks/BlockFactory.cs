using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Requests;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.Blocks
{
    public class BlockFactory : IBlockFactory
    {
        private readonly IBlockService _blockService;
        private readonly IRangeBlockService _rangeBlockService;

        public BlockFactory(IBlockService blockService, IRangeBlockService rangeBlockService)
        {
            _blockService = blockService;
            _rangeBlockService = rangeBlockService;
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

        private FindDeadRangeBlocksRequest CreateDeadBlocksRequest(BlockRequest blockRequest, int blockCountLimit)
        {
            var utcNow = DateTime.UtcNow;
            
            if (blockRequest.TaskDeathMode == TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate)
            {
                return new FindDeadRangeBlocksRequest(blockRequest.ApplicationName,
                    blockRequest.TaskName,
                    blockRequest.TaskExecutionId,
                    blockRequest.RangeType,
                    utcNow.AddSeconds(-blockRequest.GoBackElapsedSecondsForDeadTasks),
                    utcNow.AddSeconds(-blockRequest.OverrideElapsedSecondsToBeDead),
                    blockCountLimit
                    );
            }
            
            if (blockRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                return new FindDeadRangeBlocksRequest(blockRequest.ApplicationName,
                    blockRequest.TaskName,
                    blockRequest.TaskExecutionId,
                    blockRequest.RangeType,
                    utcNow.AddSeconds(-blockRequest.KeepAliveElapsedSecondsToBeDead),
                    blockCountLimit
                    );
            }

            throw new NotSupportedException("TaskDeathMode not supported");
        }

        private List<IRangeBlockContext> GetFailedBlocks(BlockRequest blockRequest, int blockCountLimit)
        {
            var failedBlockRequest = new FindFailedRangeBlocksRequest(blockRequest.ApplicationName,
                blockRequest.TaskName,
                blockRequest.TaskExecutionId,
                blockRequest.RangeType,
                DateTime.UtcNow.AddSeconds(-blockRequest.GoBackElapsedSecondsForFailedTasks),
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
            var createRequest = new RangeBlockExecutionCreateRequest(blockRequest.ApplicationName,
                    blockRequest.TaskName,
                    blockRequest.TaskExecutionId,
                    blockRequest.RangeType,
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
    }
}
