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
        public int TaskExecutionId { get; set; }
        
        public int MaxBlocks { get; set; }
        
        public bool CheckForFailedExecutions { get; set; }
        public int GoBackElapsedSecondsForFailedTasks { get; set; }
        
        public TaskDeathMode TaskDeathMode { get; set; }
        public bool CheckForDeadExecutions { get; set; }
        public int OverrideElapsedSecondsToBeDead { get; set; }
        public int GoBackElapsedSecondsForDeadTasks { get; set; }
        public int KeepAliveElapsedSecondsToBeDead { get; set; }

        public RangeBlockType RangeType { get; protected set; }
    }
}
