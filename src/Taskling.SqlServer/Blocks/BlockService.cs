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
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;
using Taskling.Retries;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.Blocks
{
    public class BlockService : IBlockService
    {
        private readonly string _connectionString;
        private readonly int _queryTimeout;
        private readonly string _tableSchema;
        private readonly ITaskService _taskService;

        public BlockService(SqlServerClientConnectionSettings connectionSettings, ITaskService taskService)
        {
            _connectionString = connectionSettings.ConnectionString;
            _queryTimeout = (int)connectionSettings.QueryTimeout.TotalMilliseconds;
            _tableSchema = connectionSettings.TableSchema;
            _taskService = taskService;
        }

        public IList<RangeBlock> FindFailedRangeBlocks(FindFailedRangeBlocksRequest failedBlocksRequest)
        {
            string query = string.Empty;
            switch (failedBlocksRequest.RangeType)
            {
                case RangeBlockType.DateRange:
                    query = RangeBlockQueryBuilder.GetFindFailedDateRangeBlocksQuery(failedBlocksRequest.BlockCountLimit, _tableSchema);
                    break;
                case RangeBlockType.NumericRange:
                    query = RangeBlockQueryBuilder.GetFindFailedNumericRangeBlocksQuery(failedBlocksRequest.BlockCountLimit, _tableSchema);
                    break;
                default:
                    throw new NotSupportedException("This range type is not supported");
            }
            
            return FindFailedDateRangeBlocks(failedBlocksRequest, query);
        }

        public IList<RangeBlock> FindDeadRangeBlocks(FindDeadRangeBlocksRequest deadBlocksRequest)
        {
            string query = string.Empty;
            switch (deadBlocksRequest.RangeType)
            {
                case RangeBlockType.DateRange:
                    if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                        query = RangeBlockQueryBuilder.GetFindDeadDateRangeBlocksWithKeepAliveQuery(deadBlocksRequest.BlockCountLimit, _tableSchema);
                    else
                        query = RangeBlockQueryBuilder.GetFindDeadDateRangeBlocksQuery(deadBlocksRequest.BlockCountLimit, _tableSchema);
                    break;
                case RangeBlockType.NumericRange:
                    if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                        query = RangeBlockQueryBuilder.GetFindDeadNumericRangeBlocksWithKeepAliveQuery(deadBlocksRequest.BlockCountLimit, _tableSchema);
                    else
                        query = RangeBlockQueryBuilder.GetFindDeadNumericRangeBlocksQuery(deadBlocksRequest.BlockCountLimit, _tableSchema);
                    break;
                default:
                    throw new NotSupportedException("This range type is not supported");
            }

            return FindDeadDateRangeBlocks(deadBlocksRequest, query);
        }

        public RangeBlockCreateResponse AddRangeBlock(RangeBlockCreateRequest rangeBlockCreateRequest)
        {
            var taskDefinition = _taskService.GetTaskDefinition(rangeBlockCreateRequest.ApplicationName, rangeBlockCreateRequest.TaskName);

            var response = new RangeBlockCreateResponse();
            switch (rangeBlockCreateRequest.RangeType)
            {
                case RangeBlockType.DateRange:
                    response.Block = AddDateRangeRangeBlock(rangeBlockCreateRequest, taskDefinition.TaskSecondaryId);
                    break;
                case RangeBlockType.NumericRange:
                    response.Block = AddNumericRangeRangeBlock(rangeBlockCreateRequest, taskDefinition.TaskSecondaryId);
                    break;
                default:
                    throw new NotSupportedException("This range type is not supported");
            }

            return response;
        }

        public string AddRangeBlockExecution(RangeBlockExecutionCreateRequest executionCreateRequest)
        {
            switch (executionCreateRequest.RangeType)
            {
                case RangeBlockType.DateRange:
                    return AddDateRangeBlockExecution(executionCreateRequest);
                case RangeBlockType.NumericRange:
                    return AddNumericRangeBlockExecution(executionCreateRequest);
                default:
                    throw new NotSupportedException("This range type is not supported");
            }
        }


        private IList<RangeBlock> FindFailedDateRangeBlocks(FindFailedRangeBlocksRequest failedBlocksRequest, string query)
        {
            var results = new List<RangeBlock>();
            var taskDefinition = _taskService.GetTaskDefinition(failedBlocksRequest.ApplicationName, failedBlocksRequest.TaskName);

            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskDefinition.TaskSecondaryId;
                    command.Parameters.Add("@FailedTaskDateLimit", SqlDbType.DateTime).Value = failedBlocksRequest.FailedTaskDateLimit;
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var rangeBlockId = reader["RangeBlockId"].ToString();
                        long rangeBegin;
                        long rangeEnd;

                        if (failedBlocksRequest.RangeType == RangeBlockType.DateRange)
                        {
                            rangeBegin = DateTime.Parse(reader["FromValue"].ToString()).Ticks;
                            rangeEnd = DateTime.Parse(reader["ToValue"].ToString()).Ticks;
                        }
                        else
                        {
                            rangeBegin = long.Parse(reader["FromValue"].ToString());
                            rangeEnd = long.Parse(reader["ToValue"].ToString());
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

        private IList<RangeBlock> FindDeadDateRangeBlocks(FindDeadRangeBlocksRequest deadBlocksRequest, string query)
        {
            var results = new List<RangeBlock>();
            var taskDefinition = _taskService.GetTaskDefinition(deadBlocksRequest.ApplicationName, deadBlocksRequest.TaskName);

            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    var command = connection.CreateCommand();
                    command.CommandText = query;
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
                       
                        var rangeBlockId = reader["RangeBlockId"].ToString();
                        long rangeBegin;
                        long rangeEnd;
                        if (deadBlocksRequest.RangeType == RangeBlockType.DateRange)
                        {
                            rangeBegin = DateTime.Parse(reader["FromValue"].ToString()).Ticks;
                            rangeEnd = DateTime.Parse(reader["ToValue"].ToString()).Ticks;
                        }
                        else
                        {
                            rangeBegin = long.Parse(reader["FromValue"].ToString());
                            rangeEnd = long.Parse(reader["ToValue"].ToString());
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

        private RangeBlock AddDateRangeRangeBlock(RangeBlockCreateRequest dateRangeBlockCreateRequest, int taskSecondaryId)
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    var command = connection.CreateCommand();
                    command.CommandText = RangeBlockQueryBuilder.GetInsertDateRangeBlockQuery(_tableSchema);
                    command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                    command.Parameters.Add("@FromDate", SqlDbType.DateTime).Value = new DateTime(dateRangeBlockCreateRequest.From);
                    command.Parameters.Add("@ToDate", SqlDbType.DateTime).Value = new DateTime(dateRangeBlockCreateRequest.To);
                    var id = command.ExecuteScalar().ToString();

                    return new RangeBlock(id,
                        dateRangeBlockCreateRequest.From,
                        dateRangeBlockCreateRequest.To) 
                        { 
                            RangeType = dateRangeBlockCreateRequest.RangeType
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
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    var command = connection.CreateCommand();
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

        private string AddDateRangeBlockExecution(RangeBlockExecutionCreateRequest executionCreateRequest)
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    var command = connection.CreateCommand();
                    command.CommandText = RangeBlockQueryBuilder.GetInsertDateRangeBlockExecutionQuery(_tableSchema);
                    command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = executionCreateRequest.TaskExecutionId;
                    command.Parameters.Add("@DateRangeBlockId", SqlDbType.BigInt).Value = long.Parse(executionCreateRequest.RangeBlockId);
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

        private string AddNumericRangeBlockExecution(RangeBlockExecutionCreateRequest executionCreateRequest)
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    var command = connection.CreateCommand();
                    command.CommandText = RangeBlockQueryBuilder.GetInsertNumericRangeBlockExecutionQuery(_tableSchema);
                    command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = executionCreateRequest.TaskExecutionId;
                    command.Parameters.Add("@NumericRangeBlockId", SqlDbType.BigInt).Value = long.Parse(executionCreateRequest.RangeBlockId);
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
