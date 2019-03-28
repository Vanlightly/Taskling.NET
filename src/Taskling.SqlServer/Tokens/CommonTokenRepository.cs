using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Tasks;

namespace Taskling.SqlServer.Tokens
{
    public class CommonTokenRepository : ICommonTokenRepository
    {
        public async Task AcquireRowLockAsync(int taskDefinitionId, string taskExecutionId, SqlCommand command)
        {
            command.Parameters.Clear();
            command.CommandText = TokensQueryBuilder.AcquireLockQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        public async Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<string> taskExecutionIds, SqlCommand command)
        {
            var results = new List<TaskExecutionState>();

            command.Parameters.Clear();
            command.CommandText = TokensQueryBuilder.GetTaskExecutions(taskExecutionIds.Count);

            for (int i = 0; i < taskExecutionIds.Count; i++)
                command.Parameters.Add("@InParam" + i, SqlDbType.Int).Value = taskExecutionIds[i];

            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
            {
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    var teState = new TaskExecutionState();

                    if (reader["CompletedAt"] != DBNull.Value)
                        teState.CompletedAt = DateTime.Parse(reader["CompletedAt"].ToString());

                    if (reader["KeepAliveDeathThreshold"] != DBNull.Value)
                        teState.KeepAliveDeathThreshold = TimeSpan.Parse(reader["KeepAliveDeathThreshold"].ToString());

                    if (reader["KeepAliveInterval"] != DBNull.Value)
                        teState.KeepAliveInterval = TimeSpan.Parse(reader["KeepAliveInterval"].ToString());

                    if (reader["LastKeepAlive"] != DBNull.Value)
                        teState.LastKeepAlive = DateTime.Parse(reader["LastKeepAlive"].ToString());

                    if (reader["OverrideThreshold"] != DBNull.Value)
                        teState.OverrideThreshold = TimeSpan.Parse(reader["OverrideThreshold"].ToString());

                    teState.StartedAt = DateTime.Parse(reader["StartedAt"].ToString());
                    teState.TaskDeathMode = (TaskDeathMode)int.Parse(reader["TaskDeathMode"].ToString());
                    teState.TaskExecutionId = reader["TaskExecutionId"].ToString();
                    teState.CurrentDateTime = DateTime.Parse(reader["CurrentDateTime"].ToString());

                    results.Add(teState);
                }
            }

            return results;
        }

        public bool HasExpired(TaskExecutionState taskExecutionState)
        {
            if (taskExecutionState.CompletedAt.HasValue)
                return true;

            if (taskExecutionState.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                if (!taskExecutionState.LastKeepAlive.HasValue)
                    return true;

                TimeSpan lastKeepAliveDiff = taskExecutionState.CurrentDateTime - taskExecutionState.LastKeepAlive.Value;
                if (lastKeepAliveDiff > taskExecutionState.KeepAliveDeathThreshold)
                    return true;

                return false;
            }
            else
            {
                TimeSpan activePeriod = taskExecutionState.CurrentDateTime - taskExecutionState.StartedAt;
                if (activePeriod > taskExecutionState.OverrideThreshold)
                    return true;

                return false;
            }
        }
    }
}
