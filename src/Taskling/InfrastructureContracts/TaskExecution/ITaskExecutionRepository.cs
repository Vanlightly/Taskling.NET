using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public interface ITaskExecutionRepository
    {
        Task<TaskExecutionStartResponse> StartAsync(TaskExecutionStartRequest startRequest);
        Task<TaskExecutionCompleteResponse> CompleteAsync(TaskExecutionCompleteRequest completeRequest);
        Task CheckpointAsync(TaskExecutionCheckpointRequest taskExecutionRequest);
        Task ErrorAsync(TaskExecutionErrorRequest taskExecutionErrorRequest);
        Task SendKeepAliveAsync(SendKeepAliveRequest sendKeepAliveRequest);
        Task<TaskExecutionMetaResponse> GetLastExecutionMetasAsync(TaskExecutionMetaRequest taskExecutionMetaRequest);
    }
}
