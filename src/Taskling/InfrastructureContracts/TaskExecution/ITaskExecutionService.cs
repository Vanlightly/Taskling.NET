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
        void Checkpoint(TaskExecutionCheckpointRequest taskExecutionRequest);
        void Error(TaskExecutionErrorRequest taskExecutionErrorRequest);
        void SendKeepAlive(SendKeepAliveRequest sendKeepAliveRequest);
    }
}
