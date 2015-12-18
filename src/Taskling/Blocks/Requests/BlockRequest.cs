using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.Blocks
{
    public class BlockRequest
    {
        public string ApplicationName { get; set; }
        public string TaskName { get; set; }
        public string TaskExecutionId { get; set; }
        
        public int MaxBlocks { get; set; }
        
        public bool CheckForFailedExecutions { get; set; }
        public TimeSpan GoBackTimePeriodForFailedTasks { get; set; }
        
        public TaskDeathMode TaskDeathMode { get; set; }
        public bool CheckForDeadExecutions { get; set; }
        public TimeSpan OverrideDeathThreshold { get; set; }
        public TimeSpan GoBackTimePeriodForDeadTasks { get; set; }
        public TimeSpan KeepAliveDeathThreshold { get; set; }

        public BlockType BlockType { get; protected set; }
    }
}
