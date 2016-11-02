using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.RangeBlocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;

namespace Taskling.InfrastructureContracts.Blocks
{
    public interface IRangeBlockRepository
    {
        void ChangeStatus(BlockExecutionChangeStatusRequest changeStatusRequest);
        RangeBlock GetLastRangeBlock(LastBlockRequest lastRangeBlockRequest);
    }
}
