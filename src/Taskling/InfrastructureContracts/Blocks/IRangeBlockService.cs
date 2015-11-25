using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.InfrastructureContracts.Blocks
{
    public interface IRangeBlockService
    {
        void ChangeStatus(RangeBlockExecutionChangeStatusRequest changeStatusRequest);
    }
}
