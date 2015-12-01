using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public class TaskExecutionCompleteRequest : RequestBase
    {
        public TaskExecutionCompleteRequest(string applicationName, string taskName, string taskExecutionId, string executionTokenId)
            : base(applicationName, taskName, taskExecutionId)
        {
            ExecutionTokenId = executionTokenId;
        }

        public string ExecutionTokenId { get; set; }
        public bool UnlimitedMode { get; set; }
    }
}
