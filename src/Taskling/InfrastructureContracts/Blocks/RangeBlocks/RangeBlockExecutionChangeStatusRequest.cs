using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.Blocks.RangeBlocks
{
    public class RangeBlockExecutionChangeStatusRequest : BlockRequestBase
    {
        public RangeBlockExecutionChangeStatusRequest(string applicationName,
            string taskName,
            int taskExecutionId,
            RangeBlockType rangeType,
            string blockExecutionId,
            BlockExecutionStatus blockExecutionStatus)
            : base(applicationName, taskName, taskExecutionId, rangeType, blockExecutionId)
        {
            BlockExecutionStatus = blockExecutionStatus;
        }

        public BlockExecutionStatus BlockExecutionStatus { get; set; }
    }
}
