using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.Exceptions;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Blocks.QueryBuilders;
using Taskling.SqlServer.Configuration;

namespace Taskling.SqlServer.Blocks
{
    public class ListBlockService : DbOperationsService, IListBlockService
    {
        public ListBlockService(SqlServerClientConnectionSettings clientConnectionSettings)
            : base(clientConnectionSettings.ConnectionString, clientConnectionSettings.QueryTimeout, clientConnectionSettings.TableSchema)
        {
        }

        public void ChangeStatus(BlockExecutionChangeStatusRequest changeStatusRequest)
        {
            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = QueryTimeout;
                    command.CommandText = GetListUpdateQuery(changeStatusRequest.BlockExecutionStatus);
                    command.Parameters.Add("@ListBlockExecutionId", SqlDbType.BigInt).Value = long.Parse(changeStatusRequest.BlockExecutionId);
                    command.Parameters.Add("@BlockExecutionStatus", SqlDbType.TinyInt).Value = (byte)changeStatusRequest.BlockExecutionStatus;
                    command.ExecuteNonQuery();
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        public IList<ListBlockItem> GetListBlockItems(string listBlockId)
        {
            var results = new List<ListBlockItem>();

            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandText = ListBlockQueryBuilder.GetListBlockItemsQuery(_tableSchema);
                    command.Parameters.Add("@ListBlockId", SqlDbType.BigInt).Value = long.Parse(listBlockId);

                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var listBlock = new ListBlockItem();
                        listBlock.ListBlockItemId = reader["ListBlockItemId"].ToString();
                        listBlock.Value = reader["Value"].ToString();
                        listBlock.Status = (ListBlockItemStatus)int.Parse(reader["Status"].ToString());

                        results.Add(listBlock);
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

        public void UpdateListBlockItem(SingleUpdateRequest singeUpdateRequest)
        {
            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = QueryTimeout;
                    command.CommandText = ListBlockQueryBuilder.GetUpdateSingleBlockListItemStatus(_tableSchema);
                    command.Parameters.Add("@ListBlockId", SqlDbType.BigInt).Value = long.Parse(singeUpdateRequest.ListBlockId);
                    command.Parameters.Add("@ListBlockItemId", SqlDbType.BigInt).Value = long.Parse(singeUpdateRequest.ListBlockItem.ListBlockItemId);
                    command.Parameters.Add("@Status", SqlDbType.TinyInt).Value = (byte)singeUpdateRequest.ListBlockItem.Status;
                    command.ExecuteNonQuery();
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        public void BatchUpdateListBlockItems(BatchUpdateRequest batchUpdateRequest)
        {
            using (var connection = CreateNewConnection())
            {
                var command = connection.CreateCommand();
                var transaction = connection.BeginTransaction();
                command.Connection = connection;
                command.Transaction = transaction;
                command.CommandTimeout = QueryTimeout;

                try
                {
                    var tableName = CreateTemporaryTable(command);
                    var dt = GenerateDataTable(batchUpdateRequest.ListBlockId, batchUpdateRequest.ListBlockItems.ToList());
                    BulkLoadInTransactionOperation(dt, tableName, connection, transaction);
                    PerformBulkUpdate(command, tableName);

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

        private string GetListUpdateQuery(BlockExecutionStatus executionStatus)
        {
            if (executionStatus == BlockExecutionStatus.Completed || executionStatus == BlockExecutionStatus.Failed)
                return BlockExecutionQueryBuilder.GetSetListBlockExecutionAsCompletedQuery(_tableSchema);

            return BlockExecutionQueryBuilder.GetUpdateListBlockExecutionStatusQuery(_tableSchema);
        }

        private string CreateTemporaryTable(SqlCommand command)
        {
            var tableName = "#TempTable" + Guid.NewGuid().ToString().Replace('-', '0');
            command.Parameters.Clear();
            command.CommandText = ListBlockQueryBuilder.GetCreateTemporaryTableQuery(tableName);
            command.ExecuteNonQuery();

            return tableName;
        }

        private DataTable GenerateDataTable(string listBlockId, List<ListBlockItem> items)
        {
            var dt = new DataTable();
            dt.Columns.Add("ListBlockId", typeof(long));
            dt.Columns.Add("ListBlockItemId", typeof(long));
            dt.Columns.Add("Status", typeof(byte));

            foreach (var item in items)
            {
                var dr = dt.NewRow();
                dr["ListBlockId"] = long.Parse(listBlockId);
                dr["ListBlockItemId"] = long.Parse(item.ListBlockItemId);
                dr["Status"] = (byte)item.Status;
                dt.Rows.Add(dr);
            }

            return dt;
        }

        private void PerformBulkUpdate(SqlCommand command, string tableName)
        {
            command.Parameters.Clear();
            command.CommandText = ListBlockQueryBuilder.GetBulkUpdateBlockListItemStatus(_tableSchema, tableName);
            command.ExecuteNonQuery();
        }
    }
}
