using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.CriticalSections
{
    public class CompleteCriticalSectionRequest : RequestBase
    {
        public CompleteCriticalSectionRequest(TaskId taskId, string taskExecutionId, CriticalSectionType criticalSectionType)
            : base(taskId, taskExecutionId)
        {
            Type = criticalSectionType;
        }

        public CriticalSectionType Type { get; set; }

    }
}
