using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.Blocks.RangeBlocks
{
    public class RangeBlockCreateRequest : BlockRequestBase
    {
        public RangeBlockCreateRequest(TaskId taskId,
            string taskExecutionId,
            DateTime fromDate,
            DateTime toDate)
            : base(taskId, taskExecutionId, Taskling.Blocks.Common.BlockType.DateRange)
        {
            From = fromDate.Ticks;
            To = toDate.Ticks;
        }

        public RangeBlockCreateRequest(TaskId taskId,
            string taskExecutionId,
            long from,
            long to)
            : base(taskId, taskExecutionId, Taskling.Blocks.Common.BlockType.NumericRange)
        {
            From = from;
            To = to;
        }

        public long From { get; set; }
        public long To { get; set; }
    }
}
