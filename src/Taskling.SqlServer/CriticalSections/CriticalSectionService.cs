using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Taskling.Exceptions;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.CriticalSections
{
    public class CriticalSectionService : DbOperationsService, ICriticalSectionService
    {
        private readonly ITaskService _taskService;
        
        public CriticalSectionService(SqlServerClientConnectionSettings clientConnectionSettings,
            ITaskService taskService)
            : base(clientConnectionSettings.ConnectionString, clientConnectionSettings.QueryTimeout)
        {
            _taskService = taskService;
        }

        public StartCriticalSectionResponse Start(StartCriticalSectionRequest startRequest)
        {
            ValidateStartRequest(startRequest);
            var taskDefinition = _taskService.GetTaskDefinition(startRequest.ApplicationName, startRequest.TaskName);

            if(startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                return RequestCriticalSectionTokenWithKeepAliveMode(taskDefinition.TaskDefinitionId, startRequest.TaskExecutionId, (int)startRequest.KeepAliveDeathThreshold.Value.TotalSeconds);
            
            if(startRequest.TaskDeathMode == TaskDeathMode.Override)
                return RequestCriticalSectionTokenWithOverrideMode(taskDefinition.TaskDefinitionId, startRequest.TaskExecutionId, (int)startRequest.OverrideThreshold.Value.TotalSeconds);

            throw new ExecutionArgumentsException("TaskDeathMode not supported");
        }

        public CompleteCriticalSectionResponse Complete(CompleteCriticalSectionRequest completeRequest)
        {
            var taskDefinition = _taskService.GetTaskDefinition(completeRequest.ApplicationName, completeRequest.TaskName);
            return ReturnCriticalSectionToken(taskDefinition.TaskDefinitionId, completeRequest.TaskExecutionId);
        }

        private void ValidateStartRequest(StartCriticalSectionRequest startRequest)
        {
            if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                if (!startRequest.KeepAliveDeathThreshold.HasValue)
                    throw new ExecutionArgumentsException("KeepAliveDeathThreshold must be set when using KeepAlive mode");
            }
            else if(startRequest.TaskDeathMode == TaskDeathMode.Override)
            {
                if (!startRequest.OverrideThreshold.HasValue)
                    throw new ExecutionArgumentsException("OverrideThreshold must be set when using Override mode");
            }
        }

        private StartCriticalSectionResponse RequestCriticalSectionTokenWithKeepAliveMode(int taskDefinitionId, string taskExecutionId, int keepAliveElapsedSeconds)
        {
            var response = new StartCriticalSectionResponse();

            using (var connection = CreateNewConnection())
            {
                SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandTimeout = QueryTimeout;
                command.CommandText = TokensQueryBuilder.KeepAliveBasedCriticalSectionQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = int.Parse(taskExecutionId);
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

        private StartCriticalSectionResponse RequestCriticalSectionTokenWithOverrideMode(int taskDefinitionId, string taskExecutionId, int secondsOverride)
        {
            var response = new StartCriticalSectionResponse();

            using (var connection = CreateNewConnection())
            {
                SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandTimeout = QueryTimeout;
                command.CommandText = TokensQueryBuilder.OverrideBasedRequestCriticalSectionTokenQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = int.Parse(taskExecutionId);
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

        private CompleteCriticalSectionResponse ReturnCriticalSectionToken(int taskDefinitionId, string taskExecutionId)
        {
            var response = new CompleteCriticalSectionResponse();

            using (var connection = CreateNewConnection())
            {
                SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = TokensQueryBuilder.ReturnCriticalSectionTokenQuery;
                command.CommandTimeout = QueryTimeout;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = int.Parse(taskExecutionId);

                try
                {
                    command.ExecuteNonQuery();
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
