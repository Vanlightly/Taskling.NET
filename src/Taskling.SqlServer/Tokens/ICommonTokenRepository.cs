using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Tokens
{
    public interface ICommonTokenRepository
    {
        void AcquireRowLock(int taskDefinitionId, string taskExecutionId, SqlCommand command);
        List<TaskExecutionState> GetTaskExecutionStates(List<string> taskExecutionIds, SqlCommand command);
        bool HasExpired(TaskExecutionState taskExecutionState);
    }
}
