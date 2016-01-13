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
            : base(clientConnectionSettings.ConnectionString, clientConnectionSettings.QueryTimeout)
        {
            _taskService = taskService;
        }

        public TaskExecutionStartResponse Start(TaskExecutionStartRequest startRequest)
        {
            ValidateStartRequest(startRequest);
            var taskDefinition = _taskService.GetTaskDefinition(startRequest.ApplicationName, startRequest.TaskName);
            
            if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                var taskExecutionId = CreateKeepAliveTaskExecution(taskDefinition.TaskDefinitionId, startRequest.KeepAliveInterval.Value, startRequest.KeepAliveDeathThreshold.Value);
                RegisterEvent(taskExecutionId.ToString(), EventType.Start, null);
                return GetExecutionTokenUsingKeepAliveMode(taskDefinition.TaskDefinitionId, taskExecutionId, startRequest.KeepAliveDeathThreshold.Value);
            }
            
            if (startRequest.TaskDeathMode == TaskDeathMode.Override)
            {
                var response = GetExecutionTokenUsingOverride(taskDefinition.TaskDefinitionId, startRequest.OverrideThreshold.Value);
                RegisterEvent(response.TaskExecutionId, EventType.Start, null);
                return response;
            }

            throw new ExecutionException("Unsupported TaskDeathMode");
        }

        public TaskExecutionCompleteResponse Complete(TaskExecutionCompleteRequest completeRequest)
        {
            RegisterEvent(completeRequest.TaskExecutionId, EventType.End, null);
            return ReturnExecutionToken(completeRequest);
        }

        public void Checkpoint(TaskExecutionCheckpointRequest taskExecutionRequest)
        {
            RegisterEvent(taskExecutionRequest.TaskExecutionId, EventType.End, taskExecutionRequest.Message);
        }

        public void Error(TaskExecutionErrorRequest taskExecutionErrorRequest)
        {
            RegisterEvent(taskExecutionErrorRequest.TaskExecutionId, EventType.Error, taskExecutionErrorRequest.Error);
        }

        public void SendKeepAlive(SendKeepAliveRequest sendKeepAliveRequest)
        {
            var taskDefinition = _taskService.GetTaskDefinition(sendKeepAliveRequest.ApplicationName, sendKeepAliveRequest.TaskName);
            
            using (var connection = CreateNewConnection())
            {
                using (var command = new SqlCommand(TaskQueryBuilder.KeepAliveQuery, connection))
                {
                    command.CommandTimeout = QueryTimeout;
                    command.Parameters.Add(new SqlParameter("@TaskDefinitionId", SqlDbType.Int)).Value = taskDefinition.TaskDefinitionId;
                    command.Parameters.Add(new SqlParameter("@TaskExecutionId", SqlDbType.Int)).Value = int.Parse(sendKeepAliveRequest.TaskExecutionId);
                    command.Parameters.Add(new SqlParameter("@ExecutionTokenId", SqlDbType.Int)).Value = int.Parse(sendKeepAliveRequest.ExecutionTokenId);
                    command.ExecuteNonQuery();
                }
            }
        }


        private void ValidateStartRequest(TaskExecutionStartRequest startRequest)
        {
            if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                if (!startRequest.KeepAliveInterval.HasValue)
                    throw new ExecutionArgumentsException("KeepAliveInterval must be set when using KeepAlive mode");

                if (!startRequest.KeepAliveDeathThreshold.HasValue)
                    throw new ExecutionArgumentsException("KeepAliveDeathThreshold must be set when using KeepAlive mode");
            }
            else if (startRequest.TaskDeathMode == TaskDeathMode.Override)
            {
                if (!startRequest.OverrideThreshold.HasValue)
                    throw new ExecutionArgumentsException("OverrideThreshold must be set when using Override mode");
            }
        }

        private TaskExecutionStartResponse GetExecutionTokenUsingOverride(int taskDefinitionId, TimeSpan overrideThreshold)
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
                    var taskExecutionId = CreateOverrideTaskExecution(command, taskDefinitionId, overrideThreshold);
                    response = TryGetExecutionTokenUsingTimeOverrideMode(command, taskDefinitionId, taskExecutionId, (int)overrideThreshold.TotalSeconds);

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

        private TaskExecutionStartResponse GetExecutionTokenUsingKeepAliveMode(int taskDefinitionId, int taskExecutionId, TimeSpan keepAliveDeathThreshold)
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
                    response = TryGetExecutionTokenUsingKeepAliveMode(command, taskDefinitionId, taskExecutionId, (int)keepAliveDeathThreshold.TotalSeconds);
                    
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

        private TaskExecutionStartResponse TryGetExecutionTokenUsingKeepAliveMode(SqlCommand command, int taskDefinitionId, int taskExecutionId, int secondsElapsedTimeOut)
        {
            var response = new TaskExecutionStartResponse();

            command.Parameters.Clear();
            command.CommandText = TokensQueryBuilder.KeepAliveBasedRequestExecutionTokenQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
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

        private TaskExecutionStartResponse TryGetExecutionTokenUsingTimeOverrideMode(SqlCommand command, int taskDefinitionId, int taskExecutionId, int secondsOverride)
        {
            var response = new TaskExecutionStartResponse();

            command.Parameters.Clear();
            command.CommandText = TokensQueryBuilder.OverrideBasedRequestExecutionTokenQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
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

        private int CreateKeepAliveTaskExecution(int taskDefinitionId, TimeSpan keepAliveInterval, TimeSpan keepAliveDeathThreshold)
        {
            using (var connection = CreateNewConnection())
            {
                using (var command = new SqlCommand(TaskQueryBuilder.InsertKeepAliveTaskExecution, connection))
                {
                    command.Parameters.Add(new SqlParameter("@TaskDefinitionId", SqlDbType.Int)).Value = taskDefinitionId;
                    command.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.VarChar, 200)).Value = Environment.MachineName;
                    command.Parameters.Add(new SqlParameter("@TaskDeathMode", SqlDbType.TinyInt)).Value = (byte) TaskDeathMode.KeepAlive;
                    command.Parameters.Add(new SqlParameter("@KeepAliveInterval", SqlDbType.Time)).Value = keepAliveInterval;
                    command.Parameters.Add(new SqlParameter("@KeepAliveDeathThreshold", SqlDbType.Time)).Value = keepAliveDeathThreshold;
                    var taskExecutionId = (int) command.ExecuteScalar();

                    return taskExecutionId;
                }
            }
        }

        private int CreateOverrideTaskExecution(SqlCommand command, int taskDefinitionId, TimeSpan overrideThreshold)
        {
            command.CommandText = TaskQueryBuilder.InsertOverrideTaskExecution;
            command.Parameters.Clear();
            command.Parameters.Add(new SqlParameter("@TaskDefinitionId", SqlDbType.Int)).Value = taskDefinitionId;
            command.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.VarChar, 200)).Value = Environment.MachineName;
            command.Parameters.Add(new SqlParameter("@TaskDeathMode", SqlDbType.TinyInt)).Value = (byte)TaskDeathMode.Override;
            command.Parameters.Add(new SqlParameter("@OverrideThreshold", SqlDbType.Time)).Value = overrideThreshold;
            var taskExecutionId = (int)command.ExecuteScalar();

            RegisterEvent(taskExecutionId.ToString(), EventType.Start, null);

            return taskExecutionId;
        }

        private TaskExecutionCompleteResponse ReturnExecutionToken(TaskExecutionCompleteRequest taskExecutionCompleteRequest)
        {
            var taskDefinition = _taskService.GetTaskDefinition(taskExecutionCompleteRequest.ApplicationName, taskExecutionCompleteRequest.TaskName);

            var response = new TaskExecutionCompleteResponse();

            using (var connection = CreateNewConnection())
            {
                SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandTimeout = QueryTimeout;
                command.CommandText = TokensQueryBuilder.ReturnExecutionTokenQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
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

        private void RegisterEvent(string taskExecutionId, EventType eventType, string message)
        {
            using (var connection = CreateNewConnection())
            {
                using (var command = new SqlCommand(TaskQueryBuilder.InsertTaskExecutionEventQuery, connection))
                {
                    command.Parameters.Add(new SqlParameter("@TaskExecutionId", SqlDbType.Int)).Value = int.Parse(taskExecutionId);
                    command.Parameters.Add(new SqlParameter("@EventType", SqlDbType.Int)).Value = (int)eventType;

                    if (message == null)
                    {
                        command.Parameters.Add(new SqlParameter("@Message", SqlDbType.VarChar, 1000)).Value = DBNull.Value;
                    }
                    else
                    {
                        if (message.Length > 1000)
                            message = message.Substring(0, 1000);
                        command.Parameters.Add(new SqlParameter("@Message", SqlDbType.VarChar, 1000)).Value = message;
                    }

                    command.Parameters.Add(new SqlParameter("@EventDateTime", SqlDbType.DateTime)).Value = DateTime.UtcNow;
                    command.ExecuteNonQuery();
                }
            }

        }
    }
}
