using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Tasks;

namespace Taskling.InfrastructureContracts.CriticalSections
{
    public class StartCriticalSectionRequest : RequestBase
    {
        public StartCriticalSectionRequest(TaskId taskId,
            string taskExecutionId,
            TaskDeathMode taskDeathMode,
            CriticalSectionType criticalSectionType)
            : base(taskId, taskExecutionId)
        {
            TaskDeathMode = taskDeathMode;
            Type = criticalSectionType;
        }

        public CriticalSectionType Type { get; set; }
        public TaskDeathMode TaskDeathMode { get; set; }
        public TimeSpan? OverrideThreshold { get; set; }
        public TimeSpan? KeepAliveDeathThreshold { get; set; }
    }
}
