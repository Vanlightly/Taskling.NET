﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.Client;
using Taskling.Exceptions;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.DataObjects;

namespace Taskling.SqlServer.TaskExecution
{
    public class TaskExecutionService : ITaskExecutionService
    {
        private readonly string _connectionString;
        private readonly int _connectTimeout;
        private readonly int _queryTimeout;
        private readonly string _tableSchema;

        public TaskExecutionService(SqlServerClientConnectionSettings clientConnectionSettings)
        {
            _connectionString = clientConnectionSettings.ConnectionString;
            _connectTimeout = (int)clientConnectionSettings.ConnectTimeout.TotalMilliseconds;
            _queryTimeout = (int)clientConnectionSettings.QueryTimeout.TotalMilliseconds;
            _tableSchema = clientConnectionSettings.TableSchema;
        }

        public TaskExecutionStartResponse Start(TaskExecutionStartRequest startRequest)
        {
            ValidateStartRequest(startRequest);

            var taskDefinition = GetTaskDefinition(startRequest);
            int secondsOverride = startRequest.SecondsOverride ?? int.MaxValue;

            if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                return GetExecutionTokenUsingKeepAliveMode(taskDefinition.TaskSecondaryId, startRequest.KeepAliveElapsedSeconds.Value, secondsOverride);
            }
            
            if (startRequest.TaskDeathMode == TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate)
            {
                return GetExecutionTokenUsingOverride(taskDefinition.TaskSecondaryId, secondsOverride);
            }

            throw new ExecutionException("Unsupported TaskDeathMode");
        }

        public TaskExecutionCompleteResponse Complete(TaskExecutionCompleteRequest taskExecutionCompleteRequest)
        {
            return ReturnExecutionToken(taskExecutionCompleteRequest);
        }

        public TaskExecutionCheckpointResponse Checkpoint(TaskExecutionCheckpointRequest taskExecutionRequest)
        {
 	        throw new NotImplementedException();
        }

        public TaskExecutionErrorResponse Error(TaskExecutionErrorRequest taskExecutionErrorRequest)
        {
 	        throw new NotImplementedException();
        }

        public void SendKeepAlive(int taskExecutionId)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(TaskQueryBuilder.KeepAliveQuery(_tableSchema), connection))
                {
                    command.Parameters.Add(new SqlParameter("@TaskExecutionId", SqlDbType.Int)).Value = taskExecutionId;
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

        private TaskDefinition GetTaskDefinition(TaskExecutionStartRequest taskExecutionStartRequest)
        {
            var taskDefinition = GetTask(taskExecutionStartRequest);
            if (taskDefinition != null)
            {
                return taskDefinition;
            }
            else
            {
                return InsertNewTask(taskExecutionStartRequest);
            }
        }

        private TaskDefinition GetTask(TaskExecutionStartRequest taskExecutionStartRequest)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(TaskQueryBuilder.GetTaskQuery(_tableSchema), connection))
                {
                    command.Parameters.Add(new SqlParameter("@ApplicationName", SqlDbType.VarChar, 200)).Value = taskExecutionStartRequest.ApplicationName;
                    command.Parameters.Add(new SqlParameter("@TaskName", SqlDbType.VarChar, 200)).Value = taskExecutionStartRequest.TaskName;
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var task = new TaskDefinition();
                        task.TaskSecondaryId = int.Parse(reader["TaskSecondaryId"].ToString());

                        return task;
                    }
                }
            }

            return null;
        }

        private TaskDefinition InsertNewTask(TaskExecutionStartRequest taskExecutionStartRequest)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(TaskQueryBuilder.InsertTaskQuery(_tableSchema), connection))
                {
                    command.Parameters.Add(new SqlParameter("@ApplicationName", SqlDbType.VarChar, 200)).Value = taskExecutionStartRequest.ApplicationName;
                    command.Parameters.Add(new SqlParameter("@TaskName", SqlDbType.VarChar, 200)).Value = taskExecutionStartRequest.TaskName;

                    var task = new TaskDefinition();
                    task.TaskSecondaryId = (int)command.ExecuteScalar();
                    return task;
                }
            }
        }

        private TaskExecutionStartResponse GetExecutionTokenUsingOverride(int taskSecondaryId, int secondsOverride)
        {
            var response = new TaskExecutionStartResponse();

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                SqlTransaction myTransaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = myTransaction;
                command.CommandTimeout = _queryTimeout;
                
                try
                {
                    var taskExecutionId = CreateTaskExecution(command, taskSecondaryId);
                    response = TryGetExecutionTokenUsingTimeOverrideMode(command, taskSecondaryId, taskExecutionId, secondsOverride);

                    myTransaction.Commit();
                }
                catch (Exception ex)
                {
                    try
                    {
                        myTransaction.Rollback();
                    }
                    catch (Exception)
                    {
                        throw new ExecutionException("Failed to commit token grant request but failed to roll back. Data could be in an inconsistent state.", ex);
                    }

                    throw new ExecutionException("Failed to commit token grant request but successfully rolled back. Data is in a consistent state.", ex);
                }
            }

            return response;
        }

        private TaskExecutionStartResponse GetExecutionTokenUsingKeepAliveMode(int taskSecondaryId, int secondsElapsedTimeOut, int secondsOverride)
        {
            var response = new TaskExecutionStartResponse();

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                SqlTransaction myTransaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = myTransaction;
                command.CommandTimeout = _queryTimeout;
                
                try
                {
                    var taskExecutionId = CreateTaskExecution(command, taskSecondaryId);
                    response = TryGetExecutionTokenUsingKeepAliveMode(command, taskSecondaryId, taskExecutionId, secondsElapsedTimeOut, secondsOverride);

                    myTransaction.Commit();
                }
                catch (Exception ex)
                {
                    try
                    {
                        myTransaction.Rollback();
                    }
                    catch (Exception)
                    {
                        throw new ExecutionException("Failed to commit token grant request but failed to roll back. Data could be in an inconsistent state.", ex);
                    }

                    throw new ExecutionException("Failed to commit token grant request but successfully rolled back. Data is in a consistent state.", ex);
                }
            }

            return response;
        }

        private TaskExecutionStartResponse TryGetExecutionTokenUsingKeepAliveMode(SqlCommand command, int taskSecondaryId, int taskExecutionId, int secondsElapsedTimeOut, int secondsOverride)
        {
            var response = new TaskExecutionStartResponse();

            command.Parameters.Clear();
            command.CommandText = TokensQueryBuilder.KeepAliveBasedRequestExecutionTokenQuery;
            command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
            command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
            command.Parameters.Add("@KeepAliveElapsedSeconds", SqlDbType.Int).Value = secondsElapsedTimeOut;
            command.Parameters.Add("@SecondsOverride", SqlDbType.Int).Value = secondsOverride;

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    response.TaskExecutionId = taskExecutionId;
                    response.ExecutionTokenId = new Guid(reader["ExecutionTokenId"].ToString());
                    response.StartedAt = DateTime.Parse(reader["StartedAt"].ToString());
                    response.GrantStatus = (GrantStatus)int.Parse(reader["Status"].ToString());
                }
            }

            return response;
        }

        private TaskExecutionStartResponse TryGetExecutionTokenUsingTimeOverrideMode(SqlCommand command, int taskSecondaryId, int taskExecutionId, int secondsOverride)
        {
            var response = new TaskExecutionStartResponse();

            command.Parameters.Clear();
            command.CommandText = TokensQueryBuilder.OverrideBasedRequestExecutionTokenQuery;
            command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
            command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
            command.Parameters.Add("@SecondsOverride", SqlDbType.Int).Value = secondsOverride;

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    response.TaskExecutionId = taskExecutionId;
                    response.ExecutionTokenId = new Guid(reader["ExecutionTokenId"].ToString());
                    response.StartedAt = DateTime.Parse(reader["StartedAt"].ToString());
                    response.GrantStatus = (GrantStatus)int.Parse(reader["Status"].ToString());
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

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                SqlTransaction myTransaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = myTransaction;
                command.CommandTimeout = _queryTimeout;
                command.CommandText = TokensQueryBuilder.ReturnExecutionTokenQuery;
                command.Parameters.Add("@ExecutionTokenId", SqlDbType.UniqueIdentifier).Value = taskExecutionCompleteRequest.ExecutionTokenId;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionCompleteRequest.TaskExecutionId;

                try
                {
                    response.CompletedAt = DateTime.Parse(command.ExecuteScalar().ToString());
                    myTransaction.Commit();
                }
                catch (Exception ex)
                {
                    try
                    {
                        myTransaction.Rollback();
                    }
                    catch (Exception)
                    {
                        throw new ExecutionException("Failed to commit token grant request but failed to roll back. Data could be in an inconsistent state.", ex);
                    }

                    throw new ExecutionException("Failed to commit token grant request but successfully rolled back. Data is in a consistent state.", ex);
                }
            }

            return response;
        }
    }
}
