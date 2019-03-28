using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CleanUp;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.TaskExecution.QueryBuilders;

namespace Taskling.SqlServer.TaskExecution
{
    public class CleanUpRepository : DbOperationsService, ICleanUpRepository
    {
        private readonly ITaskRepository _taskRepository;

        public CleanUpRepository(ITaskRepository taskRepository)
        {
            _taskRepository = taskRepository;
        }

        public async Task<bool> CleanOldDataAsync(CleanUpRequest cleanUpRequest)
        {
            var lastCleaned = await _taskRepository.GetLastTaskCleanUpTimeAsync(cleanUpRequest.TaskId).ConfigureAwait(false);
            var periodSinceLastClean = DateTime.UtcNow - lastCleaned;

            if (periodSinceLastClean > cleanUpRequest.TimeSinceLastCleaningThreashold)
            {
                await _taskRepository.SetLastCleanedAsync(cleanUpRequest.TaskId).ConfigureAwait(false);
                var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(cleanUpRequest.TaskId).ConfigureAwait(false);
                await CleanListItemsAsync(cleanUpRequest.TaskId, taskDefinition.TaskDefinitionId, cleanUpRequest.ListItemDateThreshold).ConfigureAwait(false);
                await CleanOldDataAsync(cleanUpRequest.TaskId, taskDefinition.TaskDefinitionId, cleanUpRequest.GeneralDateThreshold).ConfigureAwait(false);
                return true;
            }

            return false;
        }

        private async Task CleanListItemsAsync(TaskId taskId, int taskDefinitionId, DateTime listItemDateThreshold)
        {
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                var blockIds = await IdentifyOldBlocksAsync(taskId, connection, taskDefinitionId, listItemDateThreshold).ConfigureAwait(false);
                foreach (var blockId in blockIds)
                    await DeleteListItemsOfBlockAsync(connection, blockId).ConfigureAwait(false);
            }
        }

        private async Task<List<long>> IdentifyOldBlocksAsync(TaskId taskId, SqlConnection connection, int taskDefinitionId, DateTime listItemDateThreshold)
        {
            var blockIds = new List<long>();
            using (var command = new SqlCommand(CleanUpQueryBuilder.IdentifyOldBlocksQuery, connection))
            {
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                command.Parameters.Add(new SqlParameter("@TaskDefinitionId", SqlDbType.Int)).Value = taskDefinitionId;
                command.Parameters.Add(new SqlParameter("@OlderThanDate", SqlDbType.DateTime)).Value = listItemDateThreshold;

                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        blockIds.Add(long.Parse(reader["BlockId"].ToString()));
                    }
                }
            }

            return blockIds;
        }

        private async Task DeleteListItemsOfBlockAsync(SqlConnection connection, long blockId)
        {
            using (var command = new SqlCommand(CleanUpQueryBuilder.DeleteListItemsOfBlockQuery, connection))
            {
                command.CommandTimeout = 120;
                command.Parameters.Add(new SqlParameter("@BlockId", SqlDbType.BigInt)).Value = blockId;
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        private async Task CleanOldDataAsync(TaskId taskId, int taskDefinitionId, DateTime generalDateThreshold)
        {
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                using (var command = new SqlCommand(CleanUpQueryBuilder.DeleteOldDataQuery, connection))
                {
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@TaskDefinitionId", SqlDbType.Int)).Value = taskDefinitionId;
                    command.Parameters.Add(new SqlParameter("@OlderThanDate", SqlDbType.DateTime)).Value = generalDateThreshold;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }


    }
}
