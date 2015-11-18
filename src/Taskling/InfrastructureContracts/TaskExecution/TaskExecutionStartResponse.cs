using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public class TaskExecutionStartResponse : ResponseBase
    {
        public TaskExecutionStartResponse()
        {}

        public TaskExecutionStartResponse(Guid executionTokenId,
            DateTime startedAt,
            GrantStatus grantStatus)
        {
            ExecutionTokenId = executionTokenId;
            StartedAt = startedAt;
            GrantStatus = grantStatus;
        }

        public int TaskExecutionId { get; set; }
        public Guid ExecutionTokenId { get; set; }
        public DateTime StartedAt { get; set; }
        public GrantStatus GrantStatus { get; set; }
    }
}
