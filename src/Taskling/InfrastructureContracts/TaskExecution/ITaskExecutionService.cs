using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public interface ITaskExecutionService
    {
        TaskExecutionStartResponse Start(TaskExecutionStartRequest startRequest);
        TaskExecutionCompleteResponse Complete(TaskExecutionCompleteRequest completeRequest);
        TaskExecutionCheckpointResponse Checkpoint(TaskExecutionCheckpointRequest taskExecutionRequest);
        TaskExecutionErrorResponse Error(TaskExecutionErrorRequest taskExecutionErrorRequest);
        void SendKeepAlive(int taskExecutionId);
    }
}
