using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;
using Taskling.Retries;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.Blocks
{
    public class RangeBlockService : IRangeBlockService
    {
        private readonly string _connectionString;
        private readonly int _queryTimeout;
        private readonly string _tableSchema;
        private readonly ITaskService _taskService;

        public RangeBlockService(SqlServerClientConnectionSettings connectionSettings, ITaskService taskService)
        {
            _connectionString = connectionSettings.ConnectionString;
            _queryTimeout = (int)connectionSettings.QueryTimeout.TotalMilliseconds;
            _tableSchema = connectionSettings.TableSchema;
            _taskService = taskService;
        }

        public void ChangeStatus(RangeBlockExecutionChangeStatusRequest changeStatusRequest)
        {
            switch (changeStatusRequest.RangeType)
            {
                case RangeBlockType.DateRange:
                    ChangeStatusOfDateRangeExecution(changeStatusRequest);
                    break;
                case RangeBlockType.NumericRange:
                    ChangeStatusOfNumericRangeExecution(changeStatusRequest);
                    break;
                default:
                    throw new NotSupportedException("This range type is not supported");
            }
        }

        private void ChangeStatusOfDateRangeExecution(RangeBlockExecutionChangeStatusRequest changeStatusRequest)
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    var command = connection.CreateCommand();
                    command.CommandText = GetDateRangeUpdateQuery(changeStatusRequest.BlockExecutionStatus);
                    command.Parameters.Add("@DateRangeBlockExecutionId", SqlDbType.BigInt).Value = long.Parse(changeStatusRequest.BlockExecutionId);
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

        private void ChangeStatusOfNumericRangeExecution(RangeBlockExecutionChangeStatusRequest changeStatusRequest)
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    var command = connection.CreateCommand();
                    command.CommandText = GetNumericRangeUpdateQuery(changeStatusRequest.BlockExecutionStatus);
                    command.Parameters.Add("@NumericRangeBlockExecutionId", SqlDbType.BigInt).Value = long.Parse(changeStatusRequest.BlockExecutionId);
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
                return RangeBlockQueryBuilder.GetSetDateRangeBlockExecutionAsCompletedQuery(_tableSchema);

            return RangeBlockQueryBuilder.GetUpdateDateRangeBlockExecutionStatusQuery(_tableSchema);
        }

        private string GetNumericRangeUpdateQuery(BlockExecutionStatus executionStatus)
        {
            if (executionStatus == BlockExecutionStatus.Completed || executionStatus == BlockExecutionStatus.Failed)
                return RangeBlockQueryBuilder.GetSetNumericRangeBlockExecutionAsCompletedQuery(_tableSchema);

            return RangeBlockQueryBuilder.GetUpdateNumericRangeBlockExecutionStatusQuery(_tableSchema);
        }
    }
}
