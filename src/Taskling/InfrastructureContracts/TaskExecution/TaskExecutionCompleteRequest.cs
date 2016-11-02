using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public class TaskExecutionCompleteRequest : RequestBase
    {
        public TaskExecutionCompleteRequest(TaskId taskId, string taskExecutionId, string executionTokenId)
            : base(taskId, taskExecutionId)
        {
            ExecutionTokenId = executionTokenId;
        }

        public string ExecutionTokenId { get; set; }
        public bool Failed { get; set; }

    }
}
