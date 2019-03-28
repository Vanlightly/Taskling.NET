using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Blocks.Common;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Blocks.QueryBuilders;
using Taskling.SqlServer.Blocks.Serialization;

namespace Taskling.SqlServer.Blocks
{
    public class ObjectBlockRepository : DbOperationsService, IObjectBlockRepository
    {
        private readonly ITaskRepository _taskRepository;

        public ObjectBlockRepository(ITaskRepository taskRepository)
        {
            _taskRepository = taskRepository;
        }

        public async Task ChangeStatusAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
        {
            await ChangeStatusOfExecutionAsync(changeStatusRequest).ConfigureAwait(false);
        }

        public async Task<ObjectBlock<T>> GetLastObjectBlockAsync<T>(LastBlockRequest lastRangeBlockRequest)
        {
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(lastRangeBlockRequest.TaskId).ConfigureAwait(false);

            try
            {
                using (var connection = await CreateNewConnectionAsync(lastRangeBlockRequest.TaskId).ConfigureAwait(false))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = ObjectBlockQueryBuilder.GetLastObjectBlock;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(lastRangeBlockRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync().ConfigureAwait(false))
                        {
                            var blockId = reader["BlockId"].ToString();
                            var objectDataXml = reader["ObjectData"].ToString();
                            T objectData = SerializedValueReader.ReadValue<T>(reader, "ObjectData", "CompressedObjectData");

                            return new ObjectBlock<T>()
                            {
                                Object = objectData,
                                ObjectBlockId = blockId
                            };
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return null;
        }


        private async Task ChangeStatusOfExecutionAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
        {
            try
            {
                using (var connection = await CreateNewConnectionAsync(changeStatusRequest.TaskId).ConfigureAwait(false))
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(changeStatusRequest.TaskId).QueryTimeoutSeconds;
                    command.CommandText = GetUpdateQuery(changeStatusRequest.BlockExecutionStatus);
                    command.Parameters.Add("@BlockExecutionId", SqlDbType.BigInt).Value = long.Parse(changeStatusRequest.BlockExecutionId);
                    command.Parameters.Add("@BlockExecutionStatus", SqlDbType.TinyInt).Value = (byte)changeStatusRequest.BlockExecutionStatus;
                    command.Parameters.Add("@ItemsCount", SqlDbType.Int).Value = changeStatusRequest.ItemsProcessed;

                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        private string GetUpdateQuery(BlockExecutionStatus executionStatus)
        {
            if (executionStatus == BlockExecutionStatus.Completed || executionStatus == BlockExecutionStatus.Failed)
                return BlockExecutionQueryBuilder.SetRangeBlockExecutionAsCompleted;

            return BlockExecutionQueryBuilder.SetBlockExecutionStatusToStarted;
        }
    }
}
