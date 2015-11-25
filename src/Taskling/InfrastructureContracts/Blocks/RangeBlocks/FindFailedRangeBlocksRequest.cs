using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.Blocks.RangeBlocks
{
    public class FindFailedRangeBlocksRequest : BlockRequestBase
    {
        public FindFailedRangeBlocksRequest(string applicationName, 
            string taskName, 
            int taskExecutionId, 
            RangeBlockType rangeType,
            DateTime failedTaskDateLimit,
            int blockCountLimit)
            : base(applicationName, taskName, taskExecutionId, rangeType)
        {
            FailedTaskDateLimit = failedTaskDateLimit;
            BlockCountLimit = blockCountLimit;
        }

        public DateTime FailedTaskDateLimit { get; set; }
        public int BlockCountLimit { get; set; }
    }
}
