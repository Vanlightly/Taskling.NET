using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Taskling.Client;
using Taskling.Exceptions;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.CriticalSections
{
    public class CriticalSectionService : ICriticalSectionService
    {
        private readonly string _connectionString;
        private readonly int _connectTimeout;
        private readonly int _queryTimeout;
        private readonly string _tableSchema;

        private readonly ITaskService _taskService;
        
        public CriticalSectionService(SqlServerClientConnectionSettings clientConnectionSettings,
            ITaskService taskService)
        {
            _connectionString = clientConnectionSettings.ConnectionString;
            _connectTimeout = (int)clientConnectionSettings.ConnectTimeout.TotalMilliseconds;
            _queryTimeout = (int)clientConnectionSettings.QueryTimeout.TotalMilliseconds;
            _tableSchema = clientConnectionSettings.TableSchema;

            _taskService = taskService;
        }

        public StartCriticalSectionResponse Start(StartCriticalSectionRequest startRequest)
        {
            ValidateStartRequest(startRequest);
            var taskDefinition = _taskService.GetTaskDefinition(startRequest.ApplicationName, startRequest.TaskName);

            if(startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                return RequestCriticalSectionTokenWithKeepAliveMode(taskDefinition.TaskSecondaryId, startRequest.TaskExecutionId, startRequest.KeepAliveElapsedSeconds.Value);
            
            if(startRequest.TaskDeathMode == TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate)
                return RequestCriticalSectionTokenWithOverrideMode(taskDefinition.TaskSecondaryId, startRequest.TaskExecutionId, startRequest.SecondsOverride.Value);

            throw new ExecutionArgumentsException("TaskDeathMode not supported");
        }

        public CompleteCriticalSectionResponse Complete(CompleteCriticalSectionRequest completeRequest)
        {
            var taskDefinition = _taskService.GetTaskDefinition(completeRequest.ApplicationName, completeRequest.TaskName);
            return ReturnCriticalSectionToken(taskDefinition.TaskSecondaryId, completeRequest.TaskExecutionId);
        }

        private void ValidateStartRequest(StartCriticalSectionRequest startRequest)
        {
            if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                if (!startRequest.KeepAliveElapsedSeconds.HasValue)
                    throw new ExecutionArgumentsException("KeepAliveElapsedSeconds must be set when using KeepAlive mode");
            }
            else if(startRequest.TaskDeathMode == TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate)
            {
                if (!startRequest.SecondsOverride.HasValue)
                    throw new ExecutionArgumentsException("SecondsOverride must be set when using Override mode");
            }
        }

        private StartCriticalSectionResponse RequestCriticalSectionTokenWithKeepAliveMode(int taskSecondaryId, int taskExecutionId, int keepAliveElapsedSeconds)
        {
            var response = new StartCriticalSectionResponse();

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                SqlTransaction myTransaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = myTransaction;
                command.CommandTimeout = _queryTimeout;
                command.CommandText = TokensQueryBuilder.GetKeepAliveBasedCriticalSectionQuery(_tableSchema);
                command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
                command.Parameters.Add("@KeepAliveElapsedSeconds", SqlDbType.Int).Value = keepAliveElapsedSeconds;

                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            response.GrantStatus = (GrantStatus)int.Parse(reader["GrantStatus"].ToString());
                        }
                    }
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
                        throw new ExecutionException("Failed to commit critical section token grant request but failed to roll back. Data could be in an inconsistent state.", ex);
                    }

                    throw new ExecutionException("Failed to commit critical section token grant request but successfully rolled back. Data is in a consistent state.", ex);
                }
            }

            return response;
        }

        private StartCriticalSectionResponse RequestCriticalSectionTokenWithOverrideMode(int taskSecondaryId, int taskExecutionId, int secondsOverride)
        {
            var response = new StartCriticalSectionResponse();

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                SqlTransaction myTransaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = myTransaction;
                command.CommandTimeout = _queryTimeout;
                command.CommandText = TokensQueryBuilder.GetOverrideBasedRequestCriticalSectionTokenQuery(_tableSchema);
                command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
                command.Parameters.Add("@SecondsOverride", SqlDbType.Int).Value = secondsOverride;

                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            response.GrantStatus = (GrantStatus)int.Parse(reader["GrantStatus"].ToString());
                        }
                    }
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
                        throw new ExecutionException("Failed to commit critical section token grant request but failed to roll back. Data could be in an inconsistent state.", ex);
                    }

                    throw new ExecutionException("Failed to commit critical section token grant request but successfully rolled back. Data is in a consistent state.", ex);
                }
            }

            return response;
        }

        private CompleteCriticalSectionResponse ReturnCriticalSectionToken(int taskSecondaryId, int taskExecutionId)
        {
            var response = new CompleteCriticalSectionResponse();

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                SqlTransaction myTransaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = myTransaction;
                command.CommandText = TokensQueryBuilder.GetReturnCriticalSectionTokenQuery(_tableSchema);
                command.CommandTimeout = _queryTimeout;
                command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;

                try
                {
                    command.ExecuteNonQuery();
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
                        throw new ExecutionException("Failed to commit critical section token return request but failed to roll back. Data could be in an inconsistent state.", ex);
                    }

                    throw new ExecutionException("Failed to commit critical section token return request but successfully rolled back. Data is in a consistent state.", ex);
                }
            }

            return response;
        }
    }
}
