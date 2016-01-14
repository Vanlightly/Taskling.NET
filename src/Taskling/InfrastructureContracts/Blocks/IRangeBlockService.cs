using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.InfrastructureContracts.Blocks
{
    public interface IRangeBlockService
    {
        void ChangeStatus(BlockExecutionChangeStatusRequest changeStatusRequest);
        RangeBlock GetLastRangeBlock(LastBlockRequest lastRangeBlockRequest);
    }
}
