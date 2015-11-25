using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.Blocks.RangeBlocks
{
    public class FindDeadRangeBlocksRequest : BlockRequestBase
    {
        public FindDeadRangeBlocksRequest(string applicationName, 
            string taskName, 
            int taskExecutionId, 
            RangeBlockType rangeType,
            DateTime searchPeriodBegin,
            DateTime searchPeriodEnd,
            int blockCountLimit)
            : base(applicationName, taskName, taskExecutionId, rangeType)
        {
            SearchPeriodBegin = searchPeriodBegin;
            SearchPeriodEnd = searchPeriodEnd;
            BlockCountLimit = blockCountLimit;
            TaskDeathMode = TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate;
        }

        public FindDeadRangeBlocksRequest(string applicationName,
            string taskName,
            int taskExecutionId,
            RangeBlockType rangeType,
            DateTime lastKeepAliveLimitDateTime,
            int blockCountLimit)
            : base(applicationName, taskName, taskExecutionId, rangeType)
        {
            LastKeepAliveLimitDateTime = lastKeepAliveLimitDateTime;
            BlockCountLimit = blockCountLimit;
            TaskDeathMode = TaskDeathMode.KeepAlive;
        }

        public DateTime SearchPeriodBegin { get; set; }
        public DateTime SearchPeriodEnd { get; set; }
        public DateTime LastKeepAliveLimitDateTime { get; set; }
        public int BlockCountLimit { get; set; }
        public TaskDeathMode TaskDeathMode { get; set; }
        
    }
}
