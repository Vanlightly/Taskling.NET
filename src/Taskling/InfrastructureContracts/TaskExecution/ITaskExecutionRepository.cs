using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public interface ITaskExecutionRepository
    {
        TaskExecutionStartResponse Start(TaskExecutionStartRequest startRequest);
        TaskExecutionCompleteResponse Complete(TaskExecutionCompleteRequest completeRequest);
        void Checkpoint(TaskExecutionCheckpointRequest taskExecutionRequest);
        void Error(TaskExecutionErrorRequest taskExecutionErrorRequest);
        void SendKeepAlive(SendKeepAliveRequest sendKeepAliveRequest);
        TaskExecutionMetaResponse GetLastExecutionMetas(TaskExecutionMetaRequest taskExecutionMetaRequest);
    }
}
