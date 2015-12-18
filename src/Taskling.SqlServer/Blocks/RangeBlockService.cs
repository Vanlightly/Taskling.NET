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
        public RangeBlockService(SqlServerClientConnectionSettings clientConnectionSettings)
            : base(clientConnectionSettings.ConnectionString, clientConnectionSettings.QueryTimeout)
        {
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
