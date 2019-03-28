using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.Tasks;

namespace Taskling.SqlServer.Tokens.CriticalSections
{
    public class CriticalSectionRepository : DbOperationsService, ICriticalSectionRepository
    {
        private readonly ITaskRepository _taskRepository;
        private readonly ICommonTokenRepository _commonTokenRepository;

        public CriticalSectionRepository(ITaskRepository taskRepository,
            ICommonTokenRepository commonTokenRepository)
        {
            _taskRepository = taskRepository;
            _commonTokenRepository = commonTokenRepository;
        }

        public async Task<StartCriticalSectionResponse> StartAsync(StartCriticalSectionRequest startRequest)
        {
            ValidateStartRequest(startRequest);
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(startRequest.TaskId).ConfigureAwait(false);
            var granted = await TryAcquireCriticalSectionAsync(startRequest.TaskId, taskDefinition.TaskDefinitionId, startRequest.TaskExecutionId, startRequest.Type).ConfigureAwait(false);

            return new StartCriticalSectionResponse()
            {
                GrantStatus = granted ? GrantStatus.Granted : GrantStatus.Denied
            };
        }

        public async Task<CompleteCriticalSectionResponse> CompleteAsync(CompleteCriticalSectionRequest completeRequest)
        {
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(completeRequest.TaskId).ConfigureAwait(false);
            return await ReturnCriticalSectionTokenAsync(completeRequest.TaskId, taskDefinition.TaskDefinitionId, completeRequest.TaskExecutionId, completeRequest.Type).ConfigureAwait(false);
        }

        private void ValidateStartRequest(StartCriticalSectionRequest startRequest)
        {
            if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                if (!startRequest.KeepAliveDeathThreshold.HasValue)
                    throw new ExecutionArgumentsException("KeepAliveDeathThreshold must be set when using KeepAlive mode");
            }
            else if (startRequest.TaskDeathMode == TaskDeathMode.Override)
            {
                if (!startRequest.OverrideThreshold.HasValue)
                    throw new ExecutionArgumentsException("OverrideThreshold must be set when using Override mode");
            }
        }

        private async Task<CompleteCriticalSectionResponse> ReturnCriticalSectionTokenAsync(TaskId taskId, int taskDefinitionId, string taskExecutionId, CriticalSectionType criticalSectionType)
        {
            var response = new CompleteCriticalSectionResponse();

            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = transaction;

                if (criticalSectionType == CriticalSectionType.User)
                    command.CommandText = TokensQueryBuilder.ReturnUserCriticalSectionTokenQuery;
                else
                    command.CommandText = TokensQueryBuilder.ReturnClientCriticalSectionTokenQuery;

                command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = int.Parse(taskExecutionId);

                try
                {
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
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

        private async Task<bool> TryAcquireCriticalSectionAsync(TaskId taskId, int taskDefinitionId, string taskExecutionId, CriticalSectionType criticalSectionType)
        {
            bool granted = false;

            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds; ;

                try
                {
                    await AcquireRowLockAsync(taskDefinitionId, taskExecutionId, command).ConfigureAwait(false);
                    var csState = await GetCriticalSectionStateAsync(taskDefinitionId, criticalSectionType, command).ConfigureAwait(false);
                    await CleanseOfExpiredExecutionsAsync(csState, command).ConfigureAwait(false);

                    if (csState.IsGranted)
                    {
                        // if the critical section is still granted to another execution after cleansing
                        // then we rejected the request. If the execution is not in the queue then we add it
                        if (!csState.ExistsInQueue(taskExecutionId))
                            csState.AddToQueue(taskExecutionId);

                        granted = false;
                    }
                    else
                    {
                        if (csState.GetQueue().Any())
                        {
                            if (csState.GetFirstExecutionIdInQueue() == taskExecutionId)
                            {
                                GrantCriticalSection(csState, taskDefinitionId, taskExecutionId, command);
                                csState.RemoveFirstInQueue();
                                granted = true;
                            }
                            else
                            {
                                // not next in queue so cannot be granted the critical section
                                granted = false;
                            }
                        }
                        else
                        {
                            GrantCriticalSection(csState, taskDefinitionId, taskExecutionId, command);
                            granted = true;
                        }
                    }

                    if (csState.HasBeenModified)
                        await UpdateCriticalSectionStateAsync(taskDefinitionId, csState, criticalSectionType, command).ConfigureAwait(false);

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

            return granted;
        }

        private async Task AcquireRowLockAsync(int taskDefinitionId, string taskExecutionId, SqlCommand command)
        {
            await _commonTokenRepository.AcquireRowLockAsync(taskDefinitionId, taskExecutionId, command).ConfigureAwait(false);
        }

        private async Task<CriticalSectionState> GetCriticalSectionStateAsync(int taskDefinitionId, CriticalSectionType criticalSectionType, SqlCommand command)
        {
            command.Parameters.Clear();
            if (criticalSectionType == CriticalSectionType.User)
                command.CommandText = TokensQueryBuilder.GetUserCriticalSectionStateQuery;
            else
                command.CommandText = TokensQueryBuilder.GetClientCriticalSectionStateQuery;

            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;

            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
            {
                var readSuccess = await reader.ReadAsync().ConfigureAwait(false);
                if (readSuccess)
                {
                    var csState = new CriticalSectionState();
                    csState.IsGranted = int.Parse(reader[GetCsStatusColumnName(criticalSectionType)].ToString()) == 0;
                    csState.GrantedToExecution = reader[GetGrantedToColumnName(criticalSectionType)].ToString();
                    csState.SetQueue(reader[GetQueueColumnName(criticalSectionType)].ToString());
                    csState.StartTrackingModifications();

                    return csState;
                }
            }

            throw new CriticalSectionException("No Task exists with id " + taskDefinitionId);
        }

        private List<string> GetActiveTaskExecutionIds(CriticalSectionState csState)
        {
            var taskExecutionIds = new List<string>();

            if (!HasEmptyGranteeValue(csState))
                taskExecutionIds.Add(csState.GrantedToExecution);

            if (csState.HasQueuedExecutions())
                taskExecutionIds.AddRange(csState.GetQueue().Select(x => x.TaskExecutionId));

            return taskExecutionIds;
        }

        private async Task CleanseOfExpiredExecutionsAsync(CriticalSectionState csState, SqlCommand command)
        {
            var csQueue = csState.GetQueue();
            var activeExecutionIds = GetActiveTaskExecutionIds(csState);
            if (activeExecutionIds.Any())
            {
                var taskExecutionStates = await GetTaskExecutionStatesAsync(activeExecutionIds, command).ConfigureAwait(false);

                CleanseCurrentGranteeIfExpired(csState, taskExecutionStates);
                CleanseQueueOfExpiredExecutions(csState, taskExecutionStates, csQueue);
            }
        }

        private void CleanseCurrentGranteeIfExpired(CriticalSectionState csState, List<TaskExecutionState> taskExecutionStates)
        {
            if (!HasEmptyGranteeValue(csState) && csState.IsGranted)
            {
                var csStateOfGranted = taskExecutionStates.First(x => x.TaskExecutionId == csState.GrantedToExecution);
                if (HasCriticalSectionExpired(csStateOfGranted))
                {
                    csState.IsGranted = false;
                }
            }
        }

        private void CleanseQueueOfExpiredExecutions(CriticalSectionState csState, List<TaskExecutionState> taskExecutionStates, List<CriticalSectionQueueItem> csQueue)
        {
            var validQueuedExecutions = (from tes in taskExecutionStates
                                         join q in csQueue on tes.TaskExecutionId equals q.TaskExecutionId
                                         where HasCriticalSectionExpired(tes) == false
                                         select q).ToList();

            if (validQueuedExecutions.Count != csQueue.Count)
            {
                var updatedQueue = new List<CriticalSectionQueueItem>();
                int newQueueIndex = 1;
                foreach (var validQueuedExecution in validQueuedExecutions.OrderBy(x => x.Index))
                    updatedQueue.Add(new CriticalSectionQueueItem(newQueueIndex, validQueuedExecution.TaskExecutionId));

                csState.UpdateQueue(updatedQueue);
            }
        }

        private bool HasEmptyGranteeValue(CriticalSectionState csState)
        {
            return string.IsNullOrEmpty(csState.GrantedToExecution) || csState.GrantedToExecution.Equals("0");
        }

        private void GrantCriticalSection(CriticalSectionState csState, int taskDefinitionId, string taskExecutionId, SqlCommand command)
        {
            csState.IsGranted = true;
            csState.GrantedToExecution = taskExecutionId;
        }

        private async Task UpdateCriticalSectionStateAsync(int taskDefinitionId, CriticalSectionState csState, CriticalSectionType criticalSectionType, SqlCommand command)
        {
            command.Parameters.Clear();

            if (criticalSectionType == CriticalSectionType.User)
                command.CommandText = TokensQueryBuilder.SetUserCriticalSectionStateQuery;
            else
                command.CommandText = TokensQueryBuilder.SetClientCriticalSectionStateQuery;

            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            command.Parameters.Add("@CsStatus", SqlDbType.Int).Value = csState.IsGranted ? 1 : 0;
            command.Parameters.Add("@CsTaskExecutionId", SqlDbType.Int).Value = csState.GrantedToExecution;
            command.Parameters.Add("@CsQueue", SqlDbType.VarChar, 8000).Value = csState.GetQueueString();
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        private async Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<string> taskExecutionIds, SqlCommand command)
        {
            return await _commonTokenRepository.GetTaskExecutionStatesAsync(taskExecutionIds, command).ConfigureAwait(false);
        }

        private bool HasCriticalSectionExpired(TaskExecutionState taskExecutionState)
        {
            return _commonTokenRepository.HasExpired(taskExecutionState);
        }

        private string GetCsStatusColumnName(CriticalSectionType criticalSectionType)
        {
            if (criticalSectionType == CriticalSectionType.User)
                return "UserCsStatus";

            return "ClientCsStatus";
        }

        private string GetGrantedToColumnName(CriticalSectionType criticalSectionType)
        {
            if (criticalSectionType == CriticalSectionType.User)
                return "UserCsTaskExecutionId";

            return "ClientCsTaskExecutionId";
        }

        private string GetQueueColumnName(CriticalSectionType criticalSectionType)
        {
            if (criticalSectionType == CriticalSectionType.User)
                return "UserCsQueue";

            return "ClientCsQueue";
        }
    }
}
