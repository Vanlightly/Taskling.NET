using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.CriticalSections
{
    public class StartCriticalSectionRequest : RequestBase
    {
        public StartCriticalSectionRequest(string applicationName, 
            string taskName,
            string taskExecutionId, 
            TaskDeathMode taskDeathMode)
            :base(applicationName, taskName, taskExecutionId)
        {
            TaskDeathMode = taskDeathMode;
        }

        public TaskDeathMode TaskDeathMode { get; set; }
        public TimeSpan? OverrideThreshold { get; set; }
        public TimeSpan? KeepAliveDeathThreshold { get; set; }
    }
}
