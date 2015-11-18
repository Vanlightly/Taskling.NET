using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.CriticalSections
{
    public class CompleteCriticalSectionRequest : RequestBase
    {
        public CompleteCriticalSectionRequest(string applicationName, string taskName, int taskExecutionId)
            : base(applicationName, taskName, taskExecutionId)
        {
            
        }

    }
}
