using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public class RequestBase
    {
        public RequestBase()
        {}

        public RequestBase(string applicationName, string taskName)
        {
            ApplicationName = applicationName;
            TaskName = taskName;
        }

        public RequestBase(string applicationName, string taskName, int taskExecutionId)
        {
            ApplicationName = applicationName;
            TaskName = taskName;
            TaskExecutionId = taskExecutionId;
        }

        public string ApplicationName { get; set; }
        public string TaskName { get; set; }
        public int TaskExecutionId { get; set; }
    }
}
