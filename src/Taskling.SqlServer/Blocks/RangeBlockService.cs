using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;
using Taskling.Retries;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.Blocks
{
    public class RangeBlockService : DbOperationsService, IRangeBlockService
    {
        private readonly ITaskService _taskService;
        
        public RangeBlockService(SqlServerClientConnectionSettings clientConnectionSettings, ITaskService taskService)
            : base(clientConnectionSettings.ConnectionString, clientConnectionSettings.QueryTimeout)
        {
            _taskService = taskService;
        }

        public void ChangeStatus(BlockExecutionChangeStatusRequest changeStatusRequest)
        {
            switch (changeStatusRequest.BlockType)
            {
                case BlockType.DateRange:
                    ChangeStatusOfDateRangeExecution(changeStatusRequest);
                    break;
                case BlockType.NumericRange:
                    ChangeStatusOfNumericRangeExecution(changeStatusRequest);
                    break;
                default:
                    throw new NotSupportedException("This range type is not supported");
            }
        }

        public RangeBlock GetLastRangeBlock(LastBlockRequest lastRangeBlockRequest)
        {
            var taskDefinition = _taskService.GetTaskDefinition(lastRangeBlockRequest.ApplicationName, lastRangeBlockRequest.TaskName);

            var query = string.Empty;
            if (lastRangeBlockRequest.BlockType == BlockType.DateRange)
                query = RangeBlockQueryBuilder.GetLastDateRangeBlock;
            else if (lastRangeBlockRequest.BlockType == BlockType.NumericRange)
                query = RangeBlockQueryBuilder.GetLastNumericRangeBlock;
            else
                throw new ArgumentException("An invalid BlockType was supplied: " + lastRangeBlockRequest.BlockType);

            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = QueryTimeout;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var rangeBlockId = reader["BlockId"].ToString();
                        long rangeBegin;
                        long rangeEnd;

                        if (lastRangeBlockRequest.BlockType == BlockType.DateRange)
                        {
                            rangeBegin = DateTime.Parse(reader["FromDate"].ToString()).Ticks;
                            rangeEnd = DateTime.Parse(reader["ToDate"].ToString()).Ticks;
                        }
                        else
                        {
                            rangeBegin = long.Parse(reader["FromNumber"].ToString());
                            rangeEnd = long.Parse(reader["ToNumber"].ToString());
                        }

                        return new RangeBlock(rangeBlockId, rangeBegin, rangeEnd);
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return new RangeBlock("0", 0, 0);
        }


        private void ChangeStatusOfDateRangeExecution(BlockExecutionChangeStatusRequest changeStatusRequest)
        {
            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = QueryTimeout;
                    command.CommandText = GetDateRangeUpdateQuery(changeStatusRequest.BlockExecutionStatus);
                    command.Parameters.Add("@BlockExecutionId", SqlDbType.BigInt).Value = long.Parse(changeStatusRequest.BlockExecutionId);
                    command.Parameters.Add("@BlockExecutionStatus", SqlDbType.TinyInt).Value = (byte)changeStatusRequest.BlockExecutionStatus;
                    command.ExecuteNonQuery();
                }
            }
            catch (SqlException sqlEx)
            {
                if(TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        private void ChangeStatusOfNumericRangeExecution(BlockExecutionChangeStatusRequest changeStatusRequest)
        {
            try
            {
                using (var connection = CreateNewConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = QueryTimeout;
                    command.CommandText = GetNumericRangeUpdateQuery(changeStatusRequest.BlockExecutionStatus);
                    command.Parameters.Add("@BlockExecutionId", SqlDbType.BigInt).Value = long.Parse(changeStatusRequest.BlockExecutionId);
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

        private string GetDateRangeUpdateQuery(BlockExecutionStatus executionStatus)
        {
            if (executionStatus == BlockExecutionStatus.Completed || executionStatus == BlockExecutionStatus.Failed)
                return BlockExecutionQueryBuilder.SetBlockExecutionAsCompleted;

            return BlockExecutionQueryBuilder.UpdateBlockExecutionStatus;
        }

        private string GetNumericRangeUpdateQuery(BlockExecutionStatus executionStatus)
        {
            if (executionStatus == BlockExecutionStatus.Completed || executionStatus == BlockExecutionStatus.Failed)
                return BlockExecutionQueryBuilder.SetBlockExecutionAsCompleted;

            return BlockExecutionQueryBuilder.UpdateBlockExecutionStatus;
        }
    }
}
