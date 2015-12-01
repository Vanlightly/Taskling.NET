using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.Blocks;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests
{
    public class FindDeadBlocksRequest : BlockRequestBase
    {
        public FindDeadBlocksRequest(string applicationName, 
            string taskName, 
            string taskExecutionId, 
            BlockType blockType,
            DateTime searchPeriodBegin,
            DateTime searchPeriodEnd,
            int blockCountLimit)
            : base(applicationName, taskName, taskExecutionId, blockType)
        {
            SearchPeriodBegin = searchPeriodBegin;
            SearchPeriodEnd = searchPeriodEnd;
            BlockCountLimit = blockCountLimit;
            TaskDeathMode = TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate;
        }

        public FindDeadBlocksRequest(string applicationName,
            string taskName,
            string taskExecutionId,
            BlockType blockType,
            DateTime lastKeepAliveLimitDateTime,
            int blockCountLimit)
            : base(applicationName, taskName, taskExecutionId, blockType)
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
