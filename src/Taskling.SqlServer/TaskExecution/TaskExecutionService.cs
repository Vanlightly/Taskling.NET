using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.Exceptions;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Configuration;

using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.TaskExecution
{
    public class TaskExecutionService : DbOperationsService, ITaskExecutionService
    {
        private readonly ITaskService _taskService;

        public TaskExecutionService(SqlServerClientConnectionSettings clientConnectionSettings,
            ITaskService taskService)
            : base(clientConnectionSettings.ConnectionString, clientConnectionSettings.QueryTimeout, clientConnectionSettings.TableSchema)
        {
            _taskService = taskService;
        }

        public TaskExecutionStartResponse Start(TaskExecutionStartRequest startRequest)
        {
            ValidateStartRequest(startRequest);

            var taskDefinition = _taskService.GetTaskDefinition(startRequest.ApplicationName, startRequest.TaskName);
            int secondsOverride = startRequest.SecondsOverride ?? int.MaxValue;

            if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                return GetExecutionTokenUsingKeepAliveMode(taskDefinition.TaskSecondaryId, startRequest.KeepAliveElapsedSeconds.Value);
            }
            
            if (startRequest.TaskDeathMode == TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate)
            {
                return GetExecutionTokenUsingOverride(taskDefinition.TaskSecondaryId, secondsOverride);
            }

            throw new ExecutionException("Unsupported TaskDeathMode");
        }

        public TaskExecutionCompleteResponse Complete(TaskExecutionCompleteRequest completeRequest)
        {
            if (completeRequest.UnlimitedMode)
            {
                return new TaskExecutionCompleteResponse()
                {
                    CompletedAt = DateTime.UtcNow
                };
            }

            return ReturnExecutionToken(completeRequest);
        }

        public TaskExecutionCheckpointResponse Checkpoint(TaskExecutionCheckpointRequest taskExecutionRequest)
        {
 	        throw new NotImplementedException();
        }

        public TaskExecutionErrorResponse Error(TaskExecutionErrorRequest taskExecutionErrorRequest)
        {
 	        throw new NotImplementedException();
        }

        public void SendKeepAlive(string taskExecutionId)
        {
            using (var connection = CreateNewConnection())
            {
                using (var command = new SqlCommand(TaskQueryBuilder.KeepAliveQuery(_tableSchema), connection))
                {
                    command.CommandTimeout = QueryTimeout;
                    command.Parameters.Add(new SqlParameter("@TaskExecutionId", SqlDbType.Int)).Value = int.Parse(taskExecutionId);
                    command.ExecuteNonQuery();
                }
            }
        }


        private void ValidateStartRequest(TaskExecutionStartRequest startRequest)
        {
            if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                if (!startRequest.KeepAliveElapsedSeconds.HasValue)
                    throw new ExecutionArgumentsException("KeepAliveElapsedSeconds must be set when using KeepAlive mode");
            }
        }

        private TaskExecutionStartResponse GetExecutionTokenUsingOverride(int taskSecondaryId, int secondsOverride)
        {
            var response = new TaskExecutionStartResponse();

            using (var connection = CreateNewConnection())
            {
                SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);
                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandTimeout = QueryTimeout;
                
                try
                {
                    var taskExecutionId = CreateTaskExecution(command, taskSecondaryId);
                    response = TryGetExecutionTokenUsingTimeOverrideMode(command, taskSecondaryId, taskExecutionId, secondsOverride);

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

            return response;
        }

        private TaskExecutionStartResponse GetExecutionTokenUsingKeepAliveMode(int taskSecondaryId, int secondsElapsedTimeOut)
        {
            var response = new TaskExecutionStartResponse();

            using (var connection = CreateNewConnection())
            {
                SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);
                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandTimeout = QueryTimeout;
                
                try
                {
                    var taskExecutionId = CreateTaskExecution(command, taskSecondaryId);
                    response = TryGetExecutionTokenUsingKeepAliveMode(command, taskSecondaryId, taskExecutionId, secondsElapsedTimeOut);
                    
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

            return response;
        }

        private TaskExecutionStartResponse TryGetExecutionTokenUsingKeepAliveMode(SqlCommand command, int taskSecondaryId, int taskExecutionId, int secondsElapsedTimeOut)
        {
            var response = new TaskExecutionStartResponse();

            command.Parameters.Clear();
            command.CommandText = TokensQueryBuilder.GetKeepAliveBasedRequestExecutionTokenQuery(_tableSchema);
            command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
            command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
            command.Parameters.Add("@KeepAliveElapsedSeconds", SqlDbType.Int).Value = secondsElapsedTimeOut;
            
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    response.TaskExecutionId = taskExecutionId.ToString();
                    response.ExecutionTokenId = reader["ExecutionTokenId"].ToString();
                    response.StartedAt = DateTime.Parse(reader["StartedAt"].ToString());
                    response.GrantStatus = (GrantStatus)int.Parse(reader["GrantStatus"].ToString());
                }
            }

            return response;
        }

        private TaskExecutionStartResponse TryGetExecutionTokenUsingTimeOverrideMode(SqlCommand command, int taskSecondaryId, int taskExecutionId, int secondsOverride)
        {
            var response = new TaskExecutionStartResponse();

            command.Parameters.Clear();
            command.CommandText = TokensQueryBuilder.GetOverrideBasedRequestExecutionTokenQuery(_tableSchema);
            command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
            command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
            command.Parameters.Add("@SecondsOverride", SqlDbType.Int).Value = secondsOverride;

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    response.TaskExecutionId = taskExecutionId.ToString();
                    response.ExecutionTokenId = reader["ExecutionTokenId"].ToString();
                    response.StartedAt = DateTime.Parse(reader["StartedAt"].ToString());
                    response.GrantStatus = (GrantStatus)int.Parse(reader["GrantStatus"].ToString());
                }
            }

            return response;
        }
        
        private int CreateTaskExecution(SqlCommand command, int taskSecondaryId)
        {
            command.CommandText = TaskQueryBuilder.InsertTaskExecution(_tableSchema);
            command.Parameters.Clear();
            command.Parameters.Add(new SqlParameter("@TaskSecondaryId", SqlDbType.Int)).Value = taskSecondaryId;
            var taskExecutionId = (int)command.ExecuteScalar();
            return taskExecutionId;
        }

        private TaskExecutionCompleteResponse ReturnExecutionToken(TaskExecutionCompleteRequest taskExecutionCompleteRequest)
        {
            var response = new TaskExecutionCompleteResponse();

            using (var connection = CreateNewConnection())
            {
                SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandTimeout = QueryTimeout;
                command.CommandText = TokensQueryBuilder.GetReturnExecutionTokenQuery(_tableSchema);
                command.Parameters.Add("@ExecutionTokenId", SqlDbType.Int).Value = int.Parse(taskExecutionCompleteRequest.ExecutionTokenId);
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionCompleteRequest.TaskExecutionId;

                try
                {
                    response.CompletedAt = DateTime.Parse(command.ExecuteScalar().ToString());
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

            return response;
        }
       
    }
}
