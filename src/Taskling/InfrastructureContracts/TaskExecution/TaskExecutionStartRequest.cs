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
            TaskDeathMode taskDeathMode,
            int? secondsOverride)
            : base(applicationName, taskName)
        {
            TaskDeathMode = taskDeathMode;
            SecondsOverride = secondsOverride;
        }


        public TaskDeathMode TaskDeathMode { get; set; }
        public int? SecondsOverride { get; set; }
        public int? KeepAliveElapsedSeconds { get; set; }
    }
}
