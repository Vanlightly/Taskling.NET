using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.Events;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Events;
using Taskling.SqlServer.TaskExecution.QueryBuilders;
using Taskling.SqlServer.Tokens.Executions;
using Taskling.Tasks;

namespace Taskling.SqlServer.TaskExecution
{
    public class TaskExecutionRepository : DbOperationsService, ITaskExecutionRepository
    {
        private readonly ITaskRepository _taskRepository;
        private readonly IExecutionTokenRepository _executionTokenRepository;
        private readonly IEventsRepository _eventsRepository;

        public TaskExecutionRepository(ITaskRepository taskRepository,
            IExecutionTokenRepository executionTokenRepository,
            IEventsRepository eventsRepository)
        {
            _taskRepository = taskRepository;
            _executionTokenRepository = executionTokenRepository;
            _eventsRepository = eventsRepository;
        }

        public TaskExecutionStartResponse Start(TaskExecutionStartRequest startRequest)
        {
            ValidateStartRequest(startRequest);
            var taskDefinition = _taskRepository.EnsureTaskDefinition(startRequest.TaskId);

            if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                return StartKeepAliveExecution(startRequest, taskDefinition.TaskDefinitionId);

            if (startRequest.TaskDeathMode == TaskDeathMode.Override)
                return StartOverrideExecution(startRequest, taskDefinition.TaskDefinitionId);

            throw new ExecutionException("Unsupported TaskDeathMode");
        }

        public TaskExecutionCompleteResponse Complete(TaskExecutionCompleteRequest completeRequest)
        {
            SetCompletedDateOnTaskExecution(completeRequest.TaskId, completeRequest.TaskExecutionId);
            RegisterEvent(completeRequest.TaskId, completeRequest.TaskExecutionId, EventType.End, null);
            return ReturnExecutionToken(completeRequest);
        }

        public void Checkpoint(TaskExecutionCheckpointRequest taskExecutionRequest)
        {
            RegisterEvent(taskExecutionRequest.TaskId, taskExecutionRequest.TaskExecutionId, EventType.CheckPoint, taskExecutionRequest.Message);
        }

        public void Error(TaskExecutionErrorRequest taskExecutionErrorRequest)
        {
            if (taskExecutionErrorRequest.TreatTaskAsFailed)
                SetTaskExecutionAsFailed(taskExecutionErrorRequest.TaskId, taskExecutionErrorRequest.TaskExecutionId);

            RegisterEvent(taskExecutionErrorRequest.TaskId, taskExecutionErrorRequest.TaskExecutionId, EventType.Error, taskExecutionErrorRequest.Error);
        }

        public void SendKeepAlive(SendKeepAliveRequest sendKeepAliveRequest)
        {
            using (var connection = CreateNewConnection(sendKeepAliveRequest.TaskId))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.KeepAliveQuery, connection))
                {
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(sendKeepAliveRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add(new SqlParameter("@TaskExecutionId", SqlDbType.Int)).Value = int.Parse(sendKeepAliveRequest.TaskExecutionId);
                    command.ExecuteNonQuery();
                }
            }
        }

        public TaskExecutionMetaResponse GetLastExecutionMetas(TaskExecutionMetaRequest taskExecutionMetaRequest)
        {
            var response = new TaskExecutionMetaResponse();
            var taskDefinition = _taskRepository.EnsureTaskDefinition(taskExecutionMetaRequest.TaskId);

            using (var connection = CreateNewConnection(taskExecutionMetaRequest.TaskId))
            {
                var command = connection.CreateCommand();
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskExecutionMetaRequest.TaskId).QueryTimeoutSeconds;
                command.CommandText = TaskQueryBuilder.GetLastExecutionQuery;
                command.Parameters.Add("Top", SqlDbType.Int).Value = taskExecutionMetaRequest.ExecutionsToRetrieve;
                command.Parameters.Add("TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var executionMeta = new TaskExecutionMetaItem();
                        executionMeta.StartedAt = (DateTime)reader["StartedAt"];

                        if (reader["CompletedAt"] != DBNull.Value)
                        {
                            executionMeta.CompletedAt = (DateTime)reader["CompletedAt"];

                            bool failed = (bool)reader["Failed"];
                            bool blocked = (bool)reader["Blocked"];

                            if (failed)
                                executionMeta.Status = TaskExecutionStatus.Failed;
                            else if (blocked)
                                executionMeta.Status = TaskExecutionStatus.Blocked;
                            else
                                executionMeta.Status = TaskExecutionStatus.Completed;
                        }
                        else
                        {
                            TaskDeathMode taskDeathMode = (TaskDeathMode)(byte)reader["TaskDeathMode"];
                            if (taskDeathMode == TaskDeathMode.KeepAlive)
                            {
                                var lastKeepAlive = (DateTime)reader["LastKeepAlive"];
                                var keepAliveThreshold = (TimeSpan)reader["KeepAliveDeathThreshold"];
                                var dbServerUtcNow = (DateTime)reader["DbServerUtcNow"];

                                var timeSinceLastKeepAlive = dbServerUtcNow - lastKeepAlive;
                                if (timeSinceLastKeepAlive > keepAliveThreshold)
                                    executionMeta.Status = TaskExecutionStatus.Dead;
                                else
                                    executionMeta.Status = TaskExecutionStatus.InProgress;
                            }
                        }

                        if (reader["ExecutionHeader"] != DBNull.Value)
                            executionMeta.Header = reader["ExecutionHeader"].ToString();

                        if (reader["ReferenceValue"] != DBNull.Value)
                            executionMeta.ReferenceValue = reader["ReferenceValue"].ToString();

                        response.Executions.Add(executionMeta);
                    }
                }
            }

            return response;
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

        private TaskExecutionStartResponse StartKeepAliveExecution(TaskExecutionStartRequest startRequest, int taskDefinitionId)
        {
            var taskExecutionId = CreateKeepAliveTaskExecution(startRequest.TaskId,
                    taskDefinitionId,
                    startRequest.KeepAliveInterval.Value,
                    startRequest.KeepAliveDeathThreshold.Value,
                    startRequest.ReferenceValue,
                    startRequest.FailedTaskRetryLimit,
                    startRequest.DeadTaskRetryLimit,
                    startRequest.TasklingVersion,
                    startRequest.TaskExecutionHeader);

            RegisterEvent(startRequest.TaskId, taskExecutionId.ToString(), EventType.Start, null);
            var tokenResponse = TryGetExecutionToken(startRequest.TaskId, taskDefinitionId, taskExecutionId, startRequest.ConcurrencyLimit);
            if (tokenResponse.GrantStatus == GrantStatus.Denied)
            { 
                SetBlockedOnTaskExecution(startRequest.TaskId, taskExecutionId.ToString());
                RegisterEvent(startRequest.TaskId, taskExecutionId.ToString(), EventType.Blocked, null);
            }

            return tokenResponse;
        }

        private TaskExecutionStartResponse StartOverrideExecution(TaskExecutionStartRequest startRequest, int taskDefinitionId)
        {
            var taskExecutionId = CreateOverrideTaskExecution(startRequest.TaskId, taskDefinitionId, startRequest.OverrideThreshold.Value, startRequest.ReferenceValue,
                startRequest.FailedTaskRetryLimit, startRequest.DeadTaskRetryLimit, startRequest.TasklingVersion, startRequest.TaskExecutionHeader);
            RegisterEvent(startRequest.TaskId, taskExecutionId.ToString(), EventType.Start, null);

            var tokenResponse = TryGetExecutionToken(startRequest.TaskId, taskDefinitionId, taskExecutionId, startRequest.ConcurrencyLimit);

            if (tokenResponse.GrantStatus == GrantStatus.Denied)
            {
                SetBlockedOnTaskExecution(startRequest.TaskId, taskExecutionId.ToString());
                RegisterEvent(startRequest.TaskId, taskExecutionId.ToString(), EventType.Blocked, null);
            }

            return tokenResponse;
        }

        private TaskExecutionStartResponse TryGetExecutionToken(TaskId taskId, int taskDefinitionId, int taskExecutionId, int concurrencyLimit)
        {
            var tokenRequest = new TokenRequest()
            {
                TaskId = taskId,
                TaskDefinitionId = taskDefinitionId,
                TaskExecutionId = taskExecutionId.ToString(),
                ConcurrencyLimit = concurrencyLimit
            };

            var tokenResponse = _executionTokenRepository.TryAcquireExecutionToken(tokenRequest);

            var response = new TaskExecutionStartResponse();
            response.ExecutionTokenId = tokenResponse.ExecutionTokenId;
            response.GrantStatus = tokenResponse.GrantStatus;
            response.StartedAt = tokenResponse.StartedAt;
            response.TaskExecutionId = taskExecutionId.ToString();

            return response;
        }

        private int CreateKeepAliveTaskExecution(TaskId taskId, int taskDefinitionId, TimeSpan keepAliveInterval, TimeSpan keepAliveDeathThreshold, string referenceValue,
            short failedTaskRetryLimit, short deadTaskRetryLimit, string tasklingVersion, string executionHeader)
        {
            using (var connection = CreateNewConnection(taskId))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.InsertKeepAliveTaskExecution, connection))
                {
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                    command.Parameters.Add(new SqlParameter("@TaskDefinitionId", SqlDbType.Int)).Value = taskDefinitionId;
                    command.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.VarChar, 200)).Value = Environment.MachineName;
                    command.Parameters.Add(new SqlParameter("@TaskDeathMode", SqlDbType.TinyInt)).Value = (byte)TaskDeathMode.KeepAlive;
                    command.Parameters.Add(new SqlParameter("@KeepAliveInterval", SqlDbType.Time)).Value = keepAliveInterval;
                    command.Parameters.Add(new SqlParameter("@KeepAliveDeathThreshold", SqlDbType.Time)).Value = keepAliveDeathThreshold;
                    command.Parameters.Add(new SqlParameter("@FailedTaskRetryLimit", SqlDbType.SmallInt)).Value = failedTaskRetryLimit;
                    command.Parameters.Add(new SqlParameter("@DeadTaskRetryLimit", SqlDbType.SmallInt)).Value = deadTaskRetryLimit;
                    command.Parameters.Add(new SqlParameter("@TasklingVersion", SqlDbType.VarChar)).Value = tasklingVersion;

                    if (executionHeader == null)
                        command.Parameters.Add(new SqlParameter("@ExecutionHeader", SqlDbType.NVarChar, -1)).Value = DBNull.Value;
                    else
                        command.Parameters.Add(new SqlParameter("@ExecutionHeader", SqlDbType.NVarChar, -1)).Value = executionHeader;

                    if (referenceValue == null)
                        command.Parameters.Add(new SqlParameter("@ReferenceValue", SqlDbType.NVarChar, 2000)).Value = DBNull.Value;
                    else
                        command.Parameters.Add(new SqlParameter("@ReferenceValue", SqlDbType.NVarChar, 2000)).Value = referenceValue;

                    var taskExecutionId = (int)command.ExecuteScalar();

                    return taskExecutionId;
                }
            }
        }

        private int CreateOverrideTaskExecution(TaskId taskId, int taskDefinitionId, TimeSpan overrideThreshold, string referenceValue,
            short failedTaskRetryLimit, short deadTaskRetryLimit, string tasklingVersion, string executionHeader)
        {
            using (var connection = CreateNewConnection(taskId))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.InsertKeepAliveTaskExecution, connection))
                {
                    command.CommandText = TaskQueryBuilder.InsertOverrideTaskExecution;
                    command.Parameters.Clear();
                    command.Parameters.Add(new SqlParameter("@TaskDefinitionId", SqlDbType.Int)).Value = taskDefinitionId;
                    command.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.VarChar, 200)).Value = Environment.MachineName;
                    command.Parameters.Add(new SqlParameter("@TaskDeathMode", SqlDbType.TinyInt)).Value = (byte)TaskDeathMode.Override;
                    command.Parameters.Add(new SqlParameter("@OverrideThreshold", SqlDbType.Time)).Value = overrideThreshold;
                    command.Parameters.Add(new SqlParameter("@FailedTaskRetryLimit", SqlDbType.SmallInt)).Value = failedTaskRetryLimit;
                    command.Parameters.Add(new SqlParameter("@DeadTaskRetryLimit", SqlDbType.SmallInt)).Value = deadTaskRetryLimit;
                    command.Parameters.Add(new SqlParameter("@TasklingVersion", SqlDbType.VarChar)).Value = tasklingVersion;

                    if (executionHeader == null)
                        command.Parameters.Add(new SqlParameter("@ExecutionHeader", SqlDbType.NVarChar, -1)).Value = DBNull.Value;
                    else
                        command.Parameters.Add(new SqlParameter("@ExecutionHeader", SqlDbType.NVarChar, -1)).Value = executionHeader;

                    if (referenceValue == null)
                        command.Parameters.Add(new SqlParameter("@ReferenceValue", SqlDbType.NVarChar, 2000)).Value = DBNull.Value;
                    else
                        command.Parameters.Add(new SqlParameter("@ReferenceValue", SqlDbType.NVarChar, 2000)).Value = referenceValue;

                    return (int)command.ExecuteScalar();
                }
            }
        }

        private TaskExecutionCompleteResponse ReturnExecutionToken(TaskExecutionCompleteRequest taskExecutionCompleteRequest)
        {
            var taskDefinition = _taskRepository.EnsureTaskDefinition(taskExecutionCompleteRequest.TaskId);

            var tokenRequest = new TokenRequest()
            {
                TaskId = taskExecutionCompleteRequest.TaskId,
                TaskDefinitionId = taskDefinition.TaskDefinitionId,
                TaskExecutionId = taskExecutionCompleteRequest.TaskExecutionId
            };

            _executionTokenRepository.ReturnExecutionToken(tokenRequest, taskExecutionCompleteRequest.ExecutionTokenId);

            var response = new TaskExecutionCompleteResponse();
            response.CompletedAt = DateTime.UtcNow;
            return response;
        }

        private void SetBlockedOnTaskExecution(TaskId taskId, string taskExecutionId)
        {
            using (var connection = CreateNewConnection(taskId))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.SetBlockedTaskExecutionQuery, connection))
                {
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                    command.Parameters.Add(new SqlParameter("@TaskExecutionId", SqlDbType.Int)).Value = int.Parse(taskExecutionId);
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SetCompletedDateOnTaskExecution(TaskId taskId, string taskExecutionId)
        {
            using (var connection = CreateNewConnection(taskId))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.SetCompletedDateOfTaskExecutionQuery, connection))
                {
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                    command.Parameters.Add(new SqlParameter("@TaskExecutionId", SqlDbType.Int)).Value = int.Parse(taskExecutionId);
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SetTaskExecutionAsFailed(TaskId taskId, string taskExecutionId)
        {
            using (var connection = CreateNewConnection(taskId))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.SetTaskExecutionAsFailedQuery, connection))
                {
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                    command.Parameters.Add(new SqlParameter("@TaskExecutionId", SqlDbType.Int)).Value = int.Parse(taskExecutionId);
                    command.ExecuteNonQuery();
                }
            }
        }

        private void RegisterEvent(TaskId taskId, string taskExecutionId, EventType eventType, string message)
        {
            _eventsRepository.LogEvent(taskId, taskExecutionId, eventType, message);
        }
    }
}
