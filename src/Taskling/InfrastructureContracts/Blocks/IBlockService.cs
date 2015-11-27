using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.InfrastructureContracts.Blocks
{
    public interface IBlockService
    {
        IList<RangeBlock> FindFailedRangeBlocks(FindFailedBlocksRequest failedBlocksRequest);
        IList<RangeBlock> FindDeadRangeBlocks(FindDeadBlocksRequest deadBlocksRequest);
        RangeBlockCreateResponse AddRangeBlock(RangeBlockCreateRequest rangeBlockCreateRequest);
        string AddRangeBlockExecution(BlockExecutionCreateRequest executionCreateRequest);

        IList<ListBlock> FindFailedListBlocks(FindFailedBlocksRequest failedBlocksRequest);
        IList<ListBlock> FindDeadListBlocks(FindDeadBlocksRequest deadBlocksRequest);
        ListBlockCreateResponse AddListBlock(ListBlockCreateRequest createRequest);
        string AddListBlockExecution(BlockExecutionCreateRequest executionCreateRequest);
    }
}
