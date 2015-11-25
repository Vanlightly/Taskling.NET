using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.Blocks.RangeBlocks
{
    public class GenerateNewRangeBlocksRequest : BlockRequestBase
    {
        public GenerateNewRangeBlocksRequest(string applicationName,
            string taskName,
            int taskExecutionId,
            long rangeBegin,
            long rangeEnd)
            : base(applicationName, taskName, taskExecutionId, RangeBlockType.NumericRange)
        {
            RangeBegin = rangeBegin;
            RangeEnd = rangeEnd;
        }

        public GenerateNewRangeBlocksRequest(string applicationName,
            string taskName,
            int taskExecutionId,
            DateTime rangeBegin,
            DateTime rangeEnd)
            : base(applicationName, taskName, taskExecutionId, RangeBlockType.DateRange)
        {
            RangeBegin = rangeBegin.Ticks;
            RangeEnd = rangeEnd.Ticks;
        }

        public long RangeBegin { get; set; }
        public long RangeEnd { get; set; }
        public int BlockCountLimit { get; set; }
    }
}
