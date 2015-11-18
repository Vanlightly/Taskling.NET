using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public class TaskExecutionCompleteRequest : RequestBase
    {
        public TaskExecutionCompleteRequest(string applicationName, string taskName, int taskExecutionId, Guid executionTokenId)
            : base(applicationName, taskName, taskExecutionId)
        {
            ExecutionTokenId = executionTokenId;
        }

        public Guid ExecutionTokenId { get; set; }
        public bool UnlimitedMode { get; set; }
    }
}
