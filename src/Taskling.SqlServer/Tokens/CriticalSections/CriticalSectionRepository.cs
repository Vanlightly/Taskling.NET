using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
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

        public StartCriticalSectionResponse Start(StartCriticalSectionRequest startRequest)
        {
            ValidateStartRequest(startRequest);
            var taskDefinition = _taskRepository.EnsureTaskDefinition(startRequest.TaskId);
            var granted = TryAcquireCriticalSection(startRequest.TaskId, taskDefinition.TaskDefinitionId, startRequest.TaskExecutionId, startRequest.Type);

            return new StartCriticalSectionResponse()
            {
                GrantStatus = granted ? GrantStatus.Granted : GrantStatus.Denied
            };
        }

        public CompleteCriticalSectionResponse Complete(CompleteCriticalSectionRequest completeRequest)
        {
            var taskDefinition = _taskRepository.EnsureTaskDefinition(completeRequest.TaskId);
            return ReturnCriticalSectionToken(completeRequest.TaskId, taskDefinition.TaskDefinitionId, completeRequest.TaskExecutionId, completeRequest.Type);
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

        private CompleteCriticalSectionResponse ReturnCriticalSectionToken(TaskId taskId, int taskDefinitionId, string taskExecutionId, CriticalSectionType criticalSectionType)
        {
            var response = new CompleteCriticalSectionResponse();

            using (var connection = CreateNewConnection(taskId))
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

        private bool TryAcquireCriticalSection(TaskId taskId, int taskDefinitionId, string taskExecutionId, CriticalSectionType criticalSectionType)
        {
            bool granted = false;

            using (var connection = CreateNewConnection(taskId))
            {
                SqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);

                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds; ;

                try
                {
                    AcquireRowLock(taskDefinitionId, taskExecutionId, command);
                    var csState = GetCriticalSectionState(taskDefinitionId, criticalSectionType, command);
                    CleanseOfExpiredExecutions(csState, command);

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
                        UpdateCriticalSectionState(taskDefinitionId, csState, criticalSectionType, command);

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

        private void AcquireRowLock(int taskDefinitionId, string taskExecutionId, SqlCommand command)
        {
            _commonTokenRepository.AcquireRowLock(taskDefinitionId, taskExecutionId, command);
        }

        private CriticalSectionState GetCriticalSectionState(int taskDefinitionId, CriticalSectionType criticalSectionType, SqlCommand command)
        {
            command.Parameters.Clear();
            if (criticalSectionType == CriticalSectionType.User)
                command.CommandText = TokensQueryBuilder.GetUserCriticalSectionStateQuery;
            else
                command.CommandText = TokensQueryBuilder.GetClientCriticalSectionStateQuery;

            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;

            using (var reader = command.ExecuteReader())
            {
                var readSuccess = reader.Read();
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

        private void CleanseOfExpiredExecutions(CriticalSectionState csState, SqlCommand command)
        {
            var csQueue = csState.GetQueue();
            var activeExecutionIds = GetActiveTaskExecutionIds(csState);
            if (activeExecutionIds.Any())
            {
                var taskExecutionStates = GetTaskExecutionStates(activeExecutionIds, command);

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

        private void UpdateCriticalSectionState(int taskDefinitionId, CriticalSectionState csState, CriticalSectionType criticalSectionType, SqlCommand command)
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
            command.ExecuteNonQuery();
        }

        private List<TaskExecutionState> GetTaskExecutionStates(List<string> taskExecutionIds, SqlCommand command)
        {
            return _commonTokenRepository.GetTaskExecutionStates(taskExecutionIds, command);
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
