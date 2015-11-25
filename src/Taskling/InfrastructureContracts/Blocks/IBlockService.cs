using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.InfrastructureContracts.Blocks
{
    public interface IBlockService
    {
        IList<RangeBlock> FindFailedRangeBlocks(FindFailedRangeBlocksRequest failedBlocksRequest);
        IList<RangeBlock> FindDeadRangeBlocks(FindDeadRangeBlocksRequest deadBlocksRequest);
        RangeBlockCreateResponse AddRangeBlock(RangeBlockCreateRequest rangeBlockCreateRequest);
        string AddRangeBlockExecution(RangeBlockExecutionCreateRequest executionCreateRequest);
        //void AddRangeBlockExecution()
    }
}
