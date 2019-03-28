using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Blocks.QueryBuilders;
using Taskling.SqlServer.Blocks.Serialization;

namespace Taskling.SqlServer.Blocks
{
    public class ListBlockRepository : DbOperationsService, IListBlockRepository
    {
        private readonly ITaskRepository _taskRepository;

        public ListBlockRepository(ITaskRepository taskRepository)
        {
            _taskRepository = taskRepository;
        }

        public async Task ChangeStatusAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
        {
            try
            {
                using (var connection = await CreateNewConnectionAsync(changeStatusRequest.TaskId).ConfigureAwait(false))
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(changeStatusRequest.TaskId).QueryTimeoutSeconds;
                    command.CommandText = GetListUpdateQuery(changeStatusRequest.BlockExecutionStatus);
                    command.Parameters.Add("@BlockExecutionId", SqlDbType.BigInt).Value = long.Parse(changeStatusRequest.BlockExecutionId);
                    command.Parameters.Add("@BlockExecutionStatus", SqlDbType.TinyInt).Value = (byte)changeStatusRequest.BlockExecutionStatus;
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

        public async Task<IList<ProtoListBlockItem>> GetListBlockItemsAsync(TaskId taskId, string listBlockId)
        {
            var results = new List<ProtoListBlockItem>();

            try
            {
                using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = ListBlockQueryBuilder.GetListBlockItems;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@BlockId", SqlDbType.BigInt).Value = long.Parse(listBlockId);

                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync().ConfigureAwait(false))
                        {
                            var listBlock = new ProtoListBlockItem();
                            listBlock.ListBlockItemId = reader["ListBlockItemId"].ToString();
                            listBlock.Value = SerializedValueReader.ReadValueAsString(reader, "Value", "CompressedValue");
                            listBlock.Status = (ItemStatus)int.Parse(reader["Status"].ToString());

                            if (reader["LastUpdated"] == DBNull.Value)
                                listBlock.LastUpdated = DateTime.MinValue;
                            else
                                listBlock.LastUpdated = reader.GetDateTime(5);

                            if (reader["StatusReason"] == DBNull.Value)
                                listBlock.StatusReason = null;
                            else
                                listBlock.StatusReason = reader.GetString(6);

                            if (reader["Step"] == DBNull.Value)
                                listBlock.Step = null;
                            else
                                listBlock.Step = reader.GetByte(7);

                            results.Add(listBlock);
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

            return results;
        }

        public async Task UpdateListBlockItemAsync(SingleUpdateRequest singeUpdateRequest)
        {
            try
            {
                using (var connection = await CreateNewConnectionAsync(singeUpdateRequest.TaskId).ConfigureAwait(false))
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(singeUpdateRequest.TaskId).QueryTimeoutSeconds;
                    command.CommandText = ListBlockQueryBuilder.UpdateSingleBlockListItemStatus;
                    command.Parameters.Add("@BlockId", SqlDbType.BigInt).Value = long.Parse(singeUpdateRequest.ListBlockId);
                    command.Parameters.Add("@ListBlockItemId", SqlDbType.BigInt).Value = long.Parse(singeUpdateRequest.ListBlockItem.ListBlockItemId);
                    command.Parameters.Add("@Status", SqlDbType.TinyInt).Value = (byte)singeUpdateRequest.ListBlockItem.Status;

                    if (singeUpdateRequest.ListBlockItem.StatusReason == null)
                        command.Parameters.Add("@StatusReason", SqlDbType.NVarChar, -1).Value = DBNull.Value;
                    else
                        command.Parameters.Add("@StatusReason", SqlDbType.NVarChar, -1).Value = singeUpdateRequest.ListBlockItem.StatusReason;

                    if (!singeUpdateRequest.ListBlockItem.Step.HasValue)
                        command.Parameters.Add("@Step", SqlDbType.TinyInt).Value = DBNull.Value;
                    else
                        command.Parameters.Add("@Step", SqlDbType.TinyInt).Value = singeUpdateRequest.ListBlockItem.Step;

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

        public async Task BatchUpdateListBlockItemsAsync(BatchUpdateRequest batchUpdateRequest)
        {
            using (var connection = await CreateNewConnectionAsync(batchUpdateRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                var transaction = connection.BeginTransaction();
                command.Connection = connection;
                command.Transaction = transaction;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(batchUpdateRequest.TaskId).QueryTimeoutSeconds; ;

                try
                {
                    var tableName = await CreateTemporaryTableAsync(command).ConfigureAwait(false);
                    var dt = GenerateDataTable(batchUpdateRequest.ListBlockId, batchUpdateRequest.ListBlockItems);
                    await BulkLoadInTransactionOperationAsync(dt, tableName, connection, transaction).ConfigureAwait(false);
                    await PerformBulkUpdateAsync(command, tableName).ConfigureAwait(false);

                    transaction.Commit();
                }
                catch (SqlException sqlEx)
                {
                    TryRollBack(transaction, sqlEx);
                }
                catch (Exception ex)
                {
                    TryRollback(transaction, ex);
                }
            }
        }

        public async Task<ProtoListBlock> GetLastListBlockAsync(LastBlockRequest lastRangeBlockRequest)
        {
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(lastRangeBlockRequest.TaskId).ConfigureAwait(false);

            try
            {
                using (var connection = await CreateNewConnectionAsync(lastRangeBlockRequest.TaskId).ConfigureAwait(false))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = ListBlockQueryBuilder.GetLastListBlock;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(lastRangeBlockRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync().ConfigureAwait(false))
                        {
                            var listBlock = new ProtoListBlock();
                            listBlock.ListBlockId = reader["BlockId"].ToString();
                            listBlock.Items = await GetListBlockItemsAsync(lastRangeBlockRequest.TaskId, listBlock.ListBlockId).ConfigureAwait(false);
                            listBlock.Header = SerializedValueReader.ReadValueAsString(reader, "ObjectData", "CompressedObjectData");

                            return listBlock;
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


        private string GetListUpdateQuery(BlockExecutionStatus executionStatus)
        {
            if (executionStatus == BlockExecutionStatus.Completed || executionStatus == BlockExecutionStatus.Failed)
                return BlockExecutionQueryBuilder.SetListBlockExecutionAsCompleted;

            return BlockExecutionQueryBuilder.SetBlockExecutionStatusToStarted;
        }

        private async Task<string> CreateTemporaryTableAsync(SqlCommand command)
        {
            var tableName = "#TempTable" + Guid.NewGuid().ToString().Replace('-', '0');
            command.Parameters.Clear();
            command.CommandText = ListBlockQueryBuilder.GetCreateTemporaryTableQuery(tableName);
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            return tableName;
        }

        private DataTable GenerateDataTable(string listBlockId, IList<ProtoListBlockItem> items)
        {
            var dt = new DataTable();
            dt.Columns.Add("BlockId", typeof(long));
            dt.Columns.Add("ListBlockItemId", typeof(long));
            dt.Columns.Add("Status", typeof(byte));
            dt.Columns.Add("StatusReason", typeof(string));
            dt.Columns.Add("Step", typeof(byte));

            foreach (var item in items)
            {
                var dr = dt.NewRow();
                dr["BlockId"] = long.Parse(listBlockId);
                dr["ListBlockItemId"] = long.Parse(item.ListBlockItemId);
                dr["Status"] = (byte)item.Status;

                if (item.StatusReason == null)
                    dr["StatusReason"] = DBNull.Value;
                else
                    dr["StatusReason"] = item.StatusReason;

                if (item.Step.HasValue)
                    dr["Step"] = item.Step;
                else
                    dr["Step"] = DBNull.Value;

                dt.Rows.Add(dr);
            }

            return dt;
        }

        private async Task PerformBulkUpdateAsync(SqlCommand command, string tableName)
        {
            command.Parameters.Clear();
            command.CommandText = ListBlockQueryBuilder.GetBulkUpdateBlockListItemStatus(tableName);
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }
}
