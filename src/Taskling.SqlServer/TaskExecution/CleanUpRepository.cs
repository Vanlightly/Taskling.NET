using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
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

        public void CleanOldData(CleanUpRequest cleanUpRequest)
        {
            var lastCleaned = _taskRepository.GetLastTaskCleanUpTime(cleanUpRequest.TaskId);
            var periodSinceLastClean = DateTime.UtcNow - lastCleaned;

            if (periodSinceLastClean > cleanUpRequest.TimeSinceLastCleaningThreashold)
            {
                _taskRepository.SetLastCleaned(cleanUpRequest.TaskId);
                var taskDefinition = _taskRepository.EnsureTaskDefinition(cleanUpRequest.TaskId);
                CleanListItems(cleanUpRequest.TaskId, taskDefinition.TaskDefinitionId, cleanUpRequest.ListItemDateThreshold);
                CleanOldData(cleanUpRequest.TaskId, taskDefinition.TaskDefinitionId, cleanUpRequest.GeneralDateThreshold);
            }
        }

        private void CleanListItems(TaskId taskId, int taskDefinitionId, DateTime listItemDateThreshold)
        {
            using (var connection = CreateNewConnection(taskId))
            {
                var blockIds = IdentifyOldBlocks(taskId, connection, taskDefinitionId, listItemDateThreshold);
                foreach (var blockId in blockIds)
                    DeleteListItemsOfBlock(connection, blockId);
            }
        }

        private List<long> IdentifyOldBlocks(TaskId taskId, SqlConnection connection, int taskDefinitionId, DateTime listItemDateThreshold)
        {
            var blockIds = new List<long>();
            using (var command = new SqlCommand(CleanUpQueryBuilder.IdentifyOldBlocksQuery, connection))
            {
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                command.Parameters.Add(new SqlParameter("@TaskDefinitionId", SqlDbType.Int)).Value = taskDefinitionId;
                command.Parameters.Add(new SqlParameter("@OlderThanDate", SqlDbType.DateTime)).Value = listItemDateThreshold;

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        blockIds.Add(long.Parse(reader["BlockId"].ToString()));
                    }
                }
            }

            return blockIds;
        }

        private void DeleteListItemsOfBlock(SqlConnection connection, long blockId)
        {
            using (var command = new SqlCommand(CleanUpQueryBuilder.DeleteListItemsOfBlockQuery, connection))
            {
                command.CommandTimeout = 120;
                command.Parameters.Add(new SqlParameter("@BlockId", SqlDbType.BigInt)).Value = blockId;
                command.ExecuteNonQuery();
            }
        }

        private void CleanOldData(TaskId taskId, int taskDefinitionId, DateTime generalDateThreshold)
        {
            using (var connection = CreateNewConnection(taskId))
            {
                using (var command = new SqlCommand(CleanUpQueryBuilder.DeleteOldDataQuery, connection))
                {
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@TaskDefinitionId", SqlDbType.Int)).Value = taskDefinitionId;
                    command.Parameters.Add(new SqlParameter("@OlderThanDate", SqlDbType.DateTime)).Value = generalDateThreshold;
                    command.ExecuteNonQuery();
                }
            }
        }


    }
}
