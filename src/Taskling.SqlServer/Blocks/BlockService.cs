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
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;
using Taskling.Retries;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Blocks.QueryBuilders;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.Blocks
{
    public class BlockService : DbOperationsService, IBlockService
    {
        private readonly ITaskService _taskService;

        public BlockService(SqlServerClientConnectionSettings clientConnectionSettings, ITaskService taskService)
            : base(clientConnectionSettings.ConnectionString, clientConnectionSettings.QueryTimeout, clientConnectionSettings.TableSchema)
        {
            _taskService = taskService;
        }

        public IList<RangeBlock> FindFailedRangeBlocks(FindFailedBlocksRequest failedBlocksRequest)
        {
            string query = string.Empty;
            switch (failedBlocksRequest.BlockType)
            {
                case BlockType.DateRange:
                    query = FailedBlocksQueryBuilder.GetFindFailedDateRangeBlocksQuery(failedBlocksRequest.BlockCountLimit, _tableSchema);
                    break;
                case BlockType.NumericRange:
                    query = FailedBlocksQueryBuilder.GetFindFailedNumericRangeBlocksQuery(failedBlocksRequest.BlockCountLimit, _tableSchema);
                    break;
                default:
                    throw new NotSupportedException("This range type is not supported");
            }

            return FindFailedDateRangeBlocks(failedBlocksRequest, query);
        }

        public IList<ListBlock> FindFailedListBlocks(FindFailedBlocksRequest failedBlocksRequest)
        {
            if (failedBlocksRequest.BlockType == BlockType.List)
            {
                var query = FailedBlocksQueryBuilder.GetFindFailedListBlocksQuery(failedBlocksRequest.BlockCountLimit, _tableSchema);
                return FindFailedListBlocks(failedBlocksRequest, query);
            }

            throw new NotSupportedException("This block type is not supported");
        }

        public IList<RangeBlock> FindDeadRangeBlocks(FindDeadBlocksRequest deadBlocksRequest)
        {
            string query = string.Empty;
            switch (deadBlocksRequest.BlockType)
            {
                case BlockType.DateRange:
                    if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                        query = DeadBlocksQueryBuilder.GetFindDeadDateRangeBlocksWithKeepAliveQuery(deadBlocksRequest.BlockCountLimit, _tableSchema);
                    else
                        query = DeadBlocksQueryBuilder.GetFindDeadDateRangeBlocksQuery(deadBlocksRequest.BlockCountLimit, _tableSchema);
                    break;
                case BlockType.NumericRange:
                    if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                        query = DeadBlocksQueryBuilder.GetFindDeadNumericRangeBlocksWithKeepAliveQuery(deadBlocksRequest.BlockCountLimit, _tableSchema);
                    else
                        query = DeadBlocksQueryBuilder.GetFindDeadNumericRangeBlocksQuery(deadBlocksRequest.BlockCountLimit, _tableSchema);
                    break;
                default:
                    throw new NotSupportedException("This range type is not supported");
            }

            return FindDeadDateRangeBlocks(deadBlocksRequest, query);
        }

        public IList<ListBlock> FindDeadListBlocks(FindDeadBlocksRequest deadBlocksRequest)
        {
            if (deadBlocksRequest.BlockType == BlockType.List)
            {
                string query = string.Empty;
                if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                    query = DeadBlocksQueryBuilder.GetFindDeadListBlocksWithKeepAliveQuery(deadBlocksRequest.BlockCountLimit, _tableSchema);
                else
                    query = DeadBlocksQueryBuilder.GetFindDeadListBlocksQuery(deadBlocksRequest.BlockCountLimit, _tableSchema);

                return FindDeadListBlocks(deadBlocksRequest, query);
            }

            throw new NotSupportedException("This block type is not supported");
        }

        public RangeBlockCreateResponse AddRangeBlock(RangeBlockCreateRequest rangeBlockCreateRequest)
        {
            var taskDefinition = _taskService.GetTaskDefinition(rangeBlockCreateRequest.ApplicationName, rangeBlockCreateRequest.TaskName);

            var response = new RangeBlockCreateResponse();
            switch (rangeBlockCreateRequest.BlockType)
            {
                case BlockType.DateRange:
                    response.Block = AddDateRangeRangeBlock(rangeBlockCreateRequest, taskDefinition.TaskSecondaryId);
                    break;
                case BlockType.NumericRange:
                    response.Block = AddNumericRangeRangeBlock(rangeBlockCreateRequest, taskDefinition.TaskSecondaryId);
                    break;
                default:
                    throw new NotSupportedException("This range type is not supported");
            }

            return response;
        }

        public ListBlockCreateResponse AddListBlock(ListBlockCreateRequest createRequest)
        {
            var taskDefinition = _taskService.GetTaskDefinition(createRequest.ApplicationName, createRequest.TaskName);

            var response = new ListBlockCreateResponse();
            if (createRequest.BlockType == BlockType.List)
            {
                var listBlockId = AddNewListBlock(taskDefinition.TaskSecondaryId);
                AddListBlockItems(listBlockId, createRequest.Values);

                // we do not populate the items here, they are lazy loaded
                response.Block = new ListBlock() { ListBlockId = listBlockId.ToString() };

                return response;
            }

            throw new NotSupportedException("This block type is not supported");
        }

        public string AddRangeBlockExecution(BlockExecutionCreateRequest executionCreateRequest)
        {
            switch (executionCreateRequest.BlockType)
            {
                case BlockType.DateRange:
                    return AddDateRangeBlockExecution(executionCreateRequest);
                case BlockType.NumericRange:
                    return AddNumericRangeBlockExecution(executionCreateRequest);
                default:
                    throw new NotSupportedException("This range type is not supported");
            }
        }

        public string AddListBlockExecution(BlockExecutionCreateRequest executionCreateRequest)
        {
            if (executionCreateRequest.BlockType == BlockType.List)
                return AddNewListBlockExecution(executionCreateRequest);

            throw new NotSupportedException("This block type is not supported");
        }


        private IList<RangeBlock> FindFailedDateRangeBlocks(FindFailedBlocksRequest failedBlocksRequest, string query)
        {
            var results = new List<RangeBlock>();
            var taskDefinition = _taskService.GetTaskDefinition(failedBlocksRequest.ApplicationName, failedBlocksRequest.TaskName);

            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = QueryTimeout;
                    command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskDefinition.TaskSecondaryId;
                    command.Parameters.Add("@FailedTaskDateLimit", SqlDbType.DateTime).Value = failedBlocksRequest.FailedTaskDateLimit;
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var rangeBlockId = reader["BlockId"].ToString();
                        long rangeBegin;
                        long rangeEnd;

                        if (failedBlocksRequest.BlockType == BlockType.DateRange)
                        {
                            rangeBegin = DateTime.Parse(reader["FromDate"].ToString()).Ticks;
                            rangeEnd = DateTime.Parse(reader["ToDate"].ToString()).Ticks;
                        }
                        else
                        {
                            rangeBegin = long.Parse(reader["FromNumber"].ToString());
                            rangeEnd = long.Parse(reader["ToNumber"].ToString());
                        }

                        results.Add(new RangeBlock(rangeBlockId, rangeBegin, rangeEnd));
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

        private IList<ListBlock> FindFailedListBlocks(FindFailedBlocksRequest failedBlocksRequest, string query)
        {
            var results = new List<ListBlock>();
            var taskDefinition = _taskService.GetTaskDefinition(failedBlocksRequest.ApplicationName, failedBlocksRequest.TaskName);

            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = QueryTimeout;
                    command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskDefinition.TaskSecondaryId;
                    command.Parameters.Add("@FailedTaskDateLimit", SqlDbType.DateTime).Value = failedBlocksRequest.FailedTaskDateLimit;
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var listBlock = new ListBlock();
                        listBlock.ListBlockId = reader["BlockId"].ToString();

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

        private IList<RangeBlock> FindDeadDateRangeBlocks(FindDeadBlocksRequest deadBlocksRequest, string query)
        {
            var results = new List<RangeBlock>();
            var taskDefinition = _taskService.GetTaskDefinition(deadBlocksRequest.ApplicationName, deadBlocksRequest.TaskName);

            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = QueryTimeout;
                    command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskDefinition.TaskSecondaryId;

                    if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                    {
                        command.Parameters.Add("@LastKeepAliveLimit", SqlDbType.DateTime).Value = deadBlocksRequest.LastKeepAliveLimitDateTime;
                    }
                    else
                    {
                        command.Parameters.Add("@SearchPeriodBegin", SqlDbType.DateTime).Value = deadBlocksRequest.SearchPeriodBegin;
                        command.Parameters.Add("@SearchPeriodEnd", SqlDbType.DateTime).Value = deadBlocksRequest.SearchPeriodEnd;
                    }

                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {

                        var rangeBlockId = reader["BlockId"].ToString();
                        long rangeBegin;
                        long rangeEnd;
                        if (deadBlocksRequest.BlockType == BlockType.DateRange)
                        {
                            rangeBegin = DateTime.Parse(reader["FromDate"].ToString()).Ticks;
                            rangeEnd = DateTime.Parse(reader["ToDate"].ToString()).Ticks;
                        }
                        else
                        {
                            rangeBegin = long.Parse(reader["FromNumber"].ToString());
                            rangeEnd = long.Parse(reader["ToNumber"].ToString());
                        }

                        results.Add(new RangeBlock(rangeBlockId, rangeBegin, rangeEnd));
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

        private IList<ListBlock> FindDeadListBlocks(FindDeadBlocksRequest deadBlocksRequest, string query)
        {
            var results = new List<ListBlock>();
            var taskDefinition = _taskService.GetTaskDefinition(deadBlocksRequest.ApplicationName, deadBlocksRequest.TaskName);

            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = QueryTimeout;
                    command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskDefinition.TaskSecondaryId;

                    if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                    {
                        command.Parameters.Add("@LastKeepAliveLimit", SqlDbType.DateTime).Value = deadBlocksRequest.LastKeepAliveLimitDateTime;
                    }
                    else
                    {
                        command.Parameters.Add("@SearchPeriodBegin", SqlDbType.DateTime).Value = deadBlocksRequest.SearchPeriodBegin;
                        command.Parameters.Add("@SearchPeriodEnd", SqlDbType.DateTime).Value = deadBlocksRequest.SearchPeriodEnd;
                    }

                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var listBlock = new ListBlock();
                        listBlock.ListBlockId = reader["BlockId"].ToString();

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

        private RangeBlock AddDateRangeRangeBlock(RangeBlockCreateRequest dateRangeBlockCreateRequest, int taskSecondaryId)
        {
            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = QueryTimeout;
                    command.CommandText = RangeBlockQueryBuilder.GetInsertDateRangeBlockQuery(_tableSchema);
                    command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                    command.Parameters.Add("@FromDate", SqlDbType.DateTime).Value = new DateTime(dateRangeBlockCreateRequest.From);
                    command.Parameters.Add("@ToDate", SqlDbType.DateTime).Value = new DateTime(dateRangeBlockCreateRequest.To);
                    var id = command.ExecuteScalar().ToString();

                    return new RangeBlock(id,
                        dateRangeBlockCreateRequest.From,
                        dateRangeBlockCreateRequest.To)
                        {
                            RangeType = dateRangeBlockCreateRequest.BlockType
                        };
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        private RangeBlock AddNumericRangeRangeBlock(RangeBlockCreateRequest dateRangeBlockCreateRequest, int taskSecondaryId)
        {
            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = QueryTimeout;
                    command.CommandText = RangeBlockQueryBuilder.GetInsertNumericRangeBlockQuery(_tableSchema);
                    command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                    command.Parameters.Add("@FromNumber", SqlDbType.BigInt).Value = dateRangeBlockCreateRequest.From;
                    command.Parameters.Add("@ToNumber", SqlDbType.BigInt).Value = dateRangeBlockCreateRequest.To;
                    var id = command.ExecuteScalar().ToString();

                    return new RangeBlock(id, dateRangeBlockCreateRequest.From, dateRangeBlockCreateRequest.To);
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        private long AddNewListBlock(int taskSecondaryId)
        {
            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = QueryTimeout;
                    command.CommandText = ListBlockQueryBuilder.GetInsertListBlockQuery(_tableSchema);
                    command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                    return (long) command.ExecuteScalar();
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        private void AddListBlockItems(long listBlockId, List<string> values)
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
                    var dt = GenerateDataTable(listBlockId, values);
                    BulkLoadInTransactionOperation(dt, _tableSchema + ".ListBlockItem", connection, transaction);

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

        private DataTable GenerateDataTable(long listBlockId, List<string> values)
        {
            var dt = new DataTable();
            dt.Columns.Add("ListBlockId", typeof(long));
            dt.Columns.Add("Value", typeof(string));
            dt.Columns.Add("Status", typeof(byte));

            foreach (var value in values)
            {
                var dr = dt.NewRow();
                dr["ListBlockId"] = listBlockId;
                dr["Value"] = value;
                dr["Status"] = 0;
                dt.Rows.Add(dr);
            }

            return dt;
        }

        private string AddDateRangeBlockExecution(BlockExecutionCreateRequest executionCreateRequest)
        {
            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = QueryTimeout;
                    command.CommandText = RangeBlockQueryBuilder.GetInsertDateRangeBlockExecutionQuery(_tableSchema);
                    command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = executionCreateRequest.TaskExecutionId;
                    command.Parameters.Add("@DateRangeBlockId", SqlDbType.BigInt).Value = long.Parse(executionCreateRequest.BlockId);
                    var id = command.ExecuteScalar().ToString();

                    return id;
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        private string AddNumericRangeBlockExecution(BlockExecutionCreateRequest executionCreateRequest)
        {
            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = QueryTimeout;
                    command.CommandText = RangeBlockQueryBuilder.GetInsertNumericRangeBlockExecutionQuery(_tableSchema);
                    command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = executionCreateRequest.TaskExecutionId;
                    command.Parameters.Add("@NumericRangeBlockId", SqlDbType.BigInt).Value = long.Parse(executionCreateRequest.BlockId);
                    var id = command.ExecuteScalar().ToString();

                    return id;
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        private string AddNewListBlockExecution(BlockExecutionCreateRequest executionCreateRequest)
        {
            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = QueryTimeout;
                    command.CommandText = ListBlockQueryBuilder.GetInsertListBlockExecutionQuery(_tableSchema);
                    command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = executionCreateRequest.TaskExecutionId;
                    command.Parameters.Add("@ListBlockId", SqlDbType.BigInt).Value = long.Parse(executionCreateRequest.BlockId);
                    var id = command.ExecuteScalar().ToString();

                    return id;
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }
    }
}
