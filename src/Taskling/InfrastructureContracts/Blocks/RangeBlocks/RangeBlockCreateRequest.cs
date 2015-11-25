using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.Blocks.RangeBlocks
{
    public class RangeBlockCreateRequest : BlockRequestBase
    {
        public RangeBlockCreateRequest(string applicationName, 
            string taskName, 
            int taskExecutionId,
            DateTime fromDate,
            DateTime toDate)
            : base(applicationName, taskName, taskExecutionId, RangeBlockType.DateRange)
        {
            From = fromDate.Ticks;
            To = toDate.Ticks;
        }

        public RangeBlockCreateRequest(string applicationName,
            string taskName,
            int taskExecutionId,
            long from,
            long to)
            : base(applicationName, taskName, taskExecutionId, RangeBlockType.NumericRange)
        {
            From = from;
            To = to;
        }

        public long From { get; set; }
        public long To { get; set; }
    }
}
