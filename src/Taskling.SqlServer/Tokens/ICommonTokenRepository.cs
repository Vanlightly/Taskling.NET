using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Taskling.SqlServer.Tokens
{
    public interface ICommonTokenRepository
    {
        Task AcquireRowLockAsync(int taskDefinitionId, string taskExecutionId, SqlCommand command);
        Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<string> taskExecutionIds, SqlCommand command);
        bool HasExpired(TaskExecutionState taskExecutionState);
    }
}
