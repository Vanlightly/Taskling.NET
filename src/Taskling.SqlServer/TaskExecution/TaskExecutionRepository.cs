using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

        public async Task<TaskExecutionStartResponse> StartAsync(TaskExecutionStartRequest startRequest)
        {
            ValidateStartRequest(startRequest);
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(startRequest.TaskId).ConfigureAwait(false);

            if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                return await StartKeepAliveExecutionAsync(startRequest, taskDefinition.TaskDefinitionId).ConfigureAwait(false);

            if (startRequest.TaskDeathMode == TaskDeathMode.Override)
                return await StartOverrideExecutionAsync(startRequest, taskDefinition.TaskDefinitionId).ConfigureAwait(false);

            throw new ExecutionException("Unsupported TaskDeathMode");
        }

        public async Task<TaskExecutionCompleteResponse> CompleteAsync(TaskExecutionCompleteRequest completeRequest)
        {
            await SetCompletedDateOnTaskExecutionAsync(completeRequest.TaskId, completeRequest.TaskExecutionId).ConfigureAwait(false);
            await RegisterEventAsync(completeRequest.TaskId, completeRequest.TaskExecutionId, EventType.End, null).ConfigureAwait(false);
            return await ReturnExecutionTokenAsync(completeRequest).ConfigureAwait(false);
        }

        public async Task CheckpointAsync(TaskExecutionCheckpointRequest taskExecutionRequest)
        {
            await RegisterEventAsync(taskExecutionRequest.TaskId, taskExecutionRequest.TaskExecutionId, EventType.CheckPoint, taskExecutionRequest.Message).ConfigureAwait(false);
        }

        public async Task ErrorAsync(TaskExecutionErrorRequest taskExecutionErrorRequest)
        {
            if (taskExecutionErrorRequest.TreatTaskAsFailed)
                await SetTaskExecutionAsFailedAsync(taskExecutionErrorRequest.TaskId, taskExecutionErrorRequest.TaskExecutionId).ConfigureAwait(false);

            await RegisterEventAsync(taskExecutionErrorRequest.TaskId, taskExecutionErrorRequest.TaskExecutionId, EventType.Error, taskExecutionErrorRequest.Error).ConfigureAwait(false);
        }

        public async Task SendKeepAliveAsync(SendKeepAliveRequest sendKeepAliveRequest)
        {
            using (var connection = await CreateNewConnectionAsync(sendKeepAliveRequest.TaskId).ConfigureAwait(false))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.KeepAliveQuery, connection))
                {
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(sendKeepAliveRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add(new SqlParameter("@TaskExecutionId", SqlDbType.Int)).Value = int.Parse(sendKeepAliveRequest.TaskExecutionId);
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }

        public async Task<TaskExecutionMetaResponse> GetLastExecutionMetasAsync(TaskExecutionMetaRequest taskExecutionMetaRequest)
        {
            var response = new TaskExecutionMetaResponse();
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(taskExecutionMetaRequest.TaskId).ConfigureAwait(false);

            using (var connection = await CreateNewConnectionAsync(taskExecutionMetaRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskExecutionMetaRequest.TaskId).QueryTimeoutSeconds;
                command.CommandText = TaskQueryBuilder.GetLastExecutionQuery;
                command.Parameters.Add("Top", SqlDbType.Int).Value = taskExecutionMetaRequest.ExecutionsToRetrieve;
                command.Parameters.Add("TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;

                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
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

        private async Task<TaskExecutionStartResponse> StartKeepAliveExecutionAsync(TaskExecutionStartRequest startRequest, int taskDefinitionId)
        {
            var taskExecutionId = await CreateKeepAliveTaskExecutionAsync(startRequest.TaskId,
                    taskDefinitionId,
                    startRequest.KeepAliveInterval.Value,
                    startRequest.KeepAliveDeathThreshold.Value,
                    startRequest.ReferenceValue,
                    startRequest.FailedTaskRetryLimit,
                    startRequest.DeadTaskRetryLimit,
                    startRequest.TasklingVersion,
                    startRequest.TaskExecutionHeader).ConfigureAwait(false);

            await RegisterEventAsync(startRequest.TaskId, taskExecutionId.ToString(), EventType.Start, null).ConfigureAwait(false);
            var tokenResponse = await TryGetExecutionTokenAsync(startRequest.TaskId, taskDefinitionId, taskExecutionId, startRequest.ConcurrencyLimit).ConfigureAwait(false);
            if (tokenResponse.GrantStatus == GrantStatus.Denied)
            { 
                await SetBlockedOnTaskExecutionAsync(startRequest.TaskId, taskExecutionId.ToString()).ConfigureAwait(false);
                if(tokenResponse.Ex == null)
                    await RegisterEventAsync(startRequest.TaskId, taskExecutionId.ToString(), EventType.Blocked, null).ConfigureAwait(false);
                else
                    await RegisterEventAsync(startRequest.TaskId, taskExecutionId.ToString(), EventType.Blocked, tokenResponse.Ex.ToString()).ConfigureAwait(false);
            }

            return tokenResponse;
        }

        private async Task<TaskExecutionStartResponse> StartOverrideExecutionAsync(TaskExecutionStartRequest startRequest, int taskDefinitionId)
        {
            var taskExecutionId = await CreateOverrideTaskExecutionAsync(startRequest.TaskId, taskDefinitionId, startRequest.OverrideThreshold.Value, 
                                        startRequest.ReferenceValue,startRequest.FailedTaskRetryLimit, startRequest.DeadTaskRetryLimit, 
                                        startRequest.TasklingVersion, startRequest.TaskExecutionHeader).ConfigureAwait(false);
            await RegisterEventAsync(startRequest.TaskId, taskExecutionId.ToString(), EventType.Start, null).ConfigureAwait(false);

            var tokenResponse = await TryGetExecutionTokenAsync(startRequest.TaskId, taskDefinitionId, taskExecutionId, startRequest.ConcurrencyLimit).ConfigureAwait(false);

            if (tokenResponse.GrantStatus == GrantStatus.Denied)
            {
                await SetBlockedOnTaskExecutionAsync(startRequest.TaskId, taskExecutionId.ToString()).ConfigureAwait(false);

                if(tokenResponse.Ex == null)
                    await RegisterEventAsync(startRequest.TaskId, taskExecutionId.ToString(), EventType.Blocked, null).ConfigureAwait(false);
                else
                    await RegisterEventAsync(startRequest.TaskId, taskExecutionId.ToString(), EventType.Blocked, tokenResponse.Ex.ToString()).ConfigureAwait(false);
            }

            return tokenResponse;
        }

        private async Task<TaskExecutionStartResponse> TryGetExecutionTokenAsync(TaskId taskId, int taskDefinitionId, int taskExecutionId, int concurrencyLimit)
        {
            var tokenRequest = new TokenRequest()
            {
                TaskId = taskId,
                TaskDefinitionId = taskDefinitionId,
                TaskExecutionId = taskExecutionId.ToString(),
                ConcurrencyLimit = concurrencyLimit
            };

            try
            {
                var tokenResponse = await _executionTokenRepository.TryAcquireExecutionTokenAsync(tokenRequest).ConfigureAwait(false);

                var response = new TaskExecutionStartResponse();
                response.ExecutionTokenId = tokenResponse.ExecutionTokenId;
                response.GrantStatus = tokenResponse.GrantStatus;
                response.StartedAt = tokenResponse.StartedAt;
                response.TaskExecutionId = taskExecutionId.ToString();

                return response;
            }
            catch(Exception ex)
            {
                var response = new TaskExecutionStartResponse();
                response.StartedAt = DateTime.UtcNow;
                response.GrantStatus = GrantStatus.Denied;
                response.ExecutionTokenId = "0";
                response.TaskExecutionId = taskExecutionId.ToString();
                response.Ex = ex;

                return response;
            }
        }

        private async Task<int> CreateKeepAliveTaskExecutionAsync(TaskId taskId, int taskDefinitionId, TimeSpan keepAliveInterval, TimeSpan keepAliveDeathThreshold, string referenceValue,
            short failedTaskRetryLimit, short deadTaskRetryLimit, string tasklingVersion, string executionHeader)
        {
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
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

                    var taskExecutionId = (int)await command.ExecuteScalarAsync().ConfigureAwait(false);

                    return taskExecutionId;
                }
            }
        }

        private async Task<int> CreateOverrideTaskExecutionAsync(TaskId taskId, int taskDefinitionId, TimeSpan overrideThreshold, string referenceValue,
            short failedTaskRetryLimit, short deadTaskRetryLimit, string tasklingVersion, string executionHeader)
        {
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
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

                    return (int)await command.ExecuteScalarAsync().ConfigureAwait(false);
                }
            }
        }

        private async Task<TaskExecutionCompleteResponse> ReturnExecutionTokenAsync(TaskExecutionCompleteRequest taskExecutionCompleteRequest)
        {
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(taskExecutionCompleteRequest.TaskId).ConfigureAwait(false);

            var tokenRequest = new TokenRequest()
            {
                TaskId = taskExecutionCompleteRequest.TaskId,
                TaskDefinitionId = taskDefinition.TaskDefinitionId,
                TaskExecutionId = taskExecutionCompleteRequest.TaskExecutionId
            };

            await _executionTokenRepository.ReturnExecutionTokenAsync(tokenRequest, taskExecutionCompleteRequest.ExecutionTokenId).ConfigureAwait(false);

            var response = new TaskExecutionCompleteResponse();
            response.CompletedAt = DateTime.UtcNow;
            return response;
        }

        private async Task SetBlockedOnTaskExecutionAsync(TaskId taskId, string taskExecutionId)
        {
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.SetBlockedTaskExecutionQuery, connection))
                {
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                    command.Parameters.Add(new SqlParameter("@TaskExecutionId", SqlDbType.Int)).Value = int.Parse(taskExecutionId);
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }

        private async Task SetCompletedDateOnTaskExecutionAsync(TaskId taskId, string taskExecutionId)
        {
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.SetCompletedDateOfTaskExecutionQuery, connection))
                {
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                    command.Parameters.Add(new SqlParameter("@TaskExecutionId", SqlDbType.Int)).Value = int.Parse(taskExecutionId);
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }

        private async Task SetTaskExecutionAsFailedAsync(TaskId taskId, string taskExecutionId)
        {
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.SetTaskExecutionAsFailedQuery, connection))
                {
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                    command.Parameters.Add(new SqlParameter("@TaskExecutionId", SqlDbType.Int)).Value = int.Parse(taskExecutionId);
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }

        private async Task RegisterEventAsync(TaskId taskId, string taskExecutionId, EventType eventType, string message)
        {
            await _eventsRepository.LogEventAsync(taskId, taskExecutionId, eventType, message).ConfigureAwait(false);
        }
    }
}
