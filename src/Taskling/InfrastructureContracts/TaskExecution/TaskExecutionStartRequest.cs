using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public class TaskExecutionStartRequest : RequestBase
    {
        public TaskExecutionStartRequest(string applicationName, 
            string taskName,
            TaskDeathMode taskDeathMode)
            : base(applicationName, taskName)
        {
            TaskDeathMode = taskDeathMode;
        }


        public TaskDeathMode TaskDeathMode { get; set; }
        public TimeSpan? OverrideThreshold { get; set; }
        public TimeSpan? KeepAliveInterval { get; set; }
        public TimeSpan? KeepAliveDeathThreshold { get; set; }
    }
}
