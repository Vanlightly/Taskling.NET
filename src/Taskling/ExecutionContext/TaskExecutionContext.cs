using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Taskling.Blocks.Common;
using Taskling.Blocks.Factories;
using Taskling.Blocks.ListBlocks;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.Blocks.Requests;
using Taskling.CleanUp;
using Taskling.Configuration;
using Taskling.Contexts;
using Taskling.CriticalSection;
using Taskling.Exceptions;
using Taskling.Fluent;
using Taskling.Fluent.ListBlocks;
using Taskling.Fluent.ObjectBlocks;
using Taskling.Fluent.RangeBlocks;
using Taskling.Fluent.Settings;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Serialization;
using Taskling.Tasks;

namespace Taskling.ExecutionContext
{
    public class TaskExecutionContext : ITaskExecutionContext
    {
        #region .: Fields and services :.

        private readonly ITaskExecutionRepository _taskExecutionRepository;
        private readonly IRangeBlockRepository _rangeBlockRepository;
        private readonly IListBlockRepository _listBlockRepository;
        private readonly IObjectBlockRepository _objectBlockRepository;
        private readonly ICriticalSectionRepository _criticalSectionRepository;
        private readonly IBlockFactory _blockFactory;
        private readonly ICleanUpService _cleanUpService;
        private readonly ITasklingConfiguration _configuration;

        private TaskExecutionInstance _taskExecutionInstance;
        private TaskExecutionOptions _taskExecutionOptions;
        private bool _startedCalled;
        private bool _completeCalled;
        private bool _executionHasFailed;
        private KeepAliveDaemon _keepAliveDaemon;
        private ICriticalSectionContext _userCriticalSectionContext;
        private ICriticalSectionContext _clientCriticalSectionContext;
        private TaskConfiguration _taskConfiguration;
        private object _taskExecutionHeader;

        private const string NotActiveMessage = @"The context is not started. Only TryStart() can be called on a not started context. The context may not be in the started state because: 
1 - Complete() was already called,
2 - TryStart() returned false due to reaching the concurrency limit. When you call TryStart() always check the bool result and only continue when the result is true.
3 - the StartTask attribute has been used in PassThrough mode and the context could not start due to reaching the concurrency limit. When you use the StartTask attribute with PassThrough mode make sure you check the IsStarted property on the context before executing the logic of the job.
";

        #endregion .: Fields and services :.

        #region .: Constructors and disposal :.

        public TaskExecutionContext(ITasklingConfiguration configuration,
            ITaskExecutionRepository taskExecutionRepository,
            ICriticalSectionRepository criticalSectionRepository,
            IBlockFactory blockFactory,
            IRangeBlockRepository rangeBlockRepository,
            IListBlockRepository listBlockRepository,
            IObjectBlockRepository objectBlockRepository,
            ICleanUpService cleanUpService,
            string applicationName,
            string taskName,
            TaskExecutionOptions taskExecutionOptions)
        {
            _configuration = configuration;
            _taskExecutionRepository = taskExecutionRepository;
            _criticalSectionRepository = criticalSectionRepository;
            _blockFactory = blockFactory;
            _rangeBlockRepository = rangeBlockRepository;
            _listBlockRepository = listBlockRepository;
            _objectBlockRepository = objectBlockRepository;
            _cleanUpService = cleanUpService;

            _taskExecutionInstance = new TaskExecutionInstance();
            _taskExecutionInstance.ApplicationName = applicationName;
            _taskExecutionInstance.TaskName = taskName;

            _taskExecutionOptions = taskExecutionOptions;

            _executionHasFailed = false;

            _taskConfiguration = _configuration.GetTaskConfiguration(applicationName, taskName);
        }

        ~TaskExecutionContext()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected bool disposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
            { }

            if (_startedCalled && !_completeCalled)
            {
                Task.Run(async () => await CompleteAsync());
            }

            disposed = true;
        }

        #endregion .: Constructors and disposal :.

        #region .: Public Properties :.

        public bool IsStarted
        {
            get
            {
                return IsExecutionContextActive;
            }
        }

        #endregion .: Public Properties :.

        #region .: Public methods :.

        public async Task<bool> TryStartAsync()
        {
            return await TryStartAsync(null);
        }

        public async Task<bool> TryStartAsync(string referenceValue)
        {
            if (!_taskExecutionOptions.Enabled)
            {
                return false;
            }

            if (_startedCalled)
                throw new ExecutionException("The execution context has already been started");

            _startedCalled = true;

            CleanUpOldData();
            var startRequest = CreateStartRequest(referenceValue);

            try
            {
                var response = await _taskExecutionRepository.StartAsync(startRequest);
                _taskExecutionInstance.TaskExecutionId = response.TaskExecutionId;
                _taskExecutionInstance.ExecutionTokenId = response.ExecutionTokenId;

                if (response.GrantStatus == GrantStatus.Denied)
                {
                    await CompleteAsync();
                    return false;
                }

                if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
                    StartKeepAlive();
            }
            catch (Exception)
            {
                _completeCalled = true;
                throw;
            }

            return true;
        }

        public async Task<bool> TryStartAsync<TExecutionHeader>(TExecutionHeader executionHeader)
        {
            _taskExecutionHeader = executionHeader;
            return await TryStartAsync();
        }

        public async Task<bool> TryStartAsync<TExecutionHeader>(TExecutionHeader executionHeader, string referenceValue)
        {
            _taskExecutionHeader = executionHeader;
            return await TryStartAsync(referenceValue);
        }

        public async Task CompleteAsync()
        {
            if (IsExecutionContextActive)
            {
                _completeCalled = true;

                if (_keepAliveDaemon != null)
                    _keepAliveDaemon.Stop();

                var completeRequest = new TaskExecutionCompleteRequest(new TaskId(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName),
                    _taskExecutionInstance.TaskExecutionId,
                    _taskExecutionInstance.ExecutionTokenId);
                completeRequest.Failed = _executionHasFailed;


                var response = await _taskExecutionRepository.CompleteAsync(completeRequest);
                _taskExecutionInstance.CompletedAt = response.CompletedAt;
            }
        }

        public async Task CheckpointAsync(string checkpointMessage)
        {
            if (!IsExecutionContextActive)
                throw new ExecutionException(NotActiveMessage);

            var request = new TaskExecutionCheckpointRequest()
            {
                TaskId = new TaskId(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName),
                TaskExecutionId = _taskExecutionInstance.TaskExecutionId,
                Message = checkpointMessage
            };
            await _taskExecutionRepository.CheckpointAsync(request);
        }

        public async Task ErrorAsync(string errorMessage, bool treatTaskAsFailed)
        {
            if (!IsExecutionContextActive)
                throw new ExecutionException(NotActiveMessage);

            _executionHasFailed = treatTaskAsFailed;

            var request = new TaskExecutionErrorRequest()
            {
                TaskId = new TaskId(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName),
                TaskExecutionId = _taskExecutionInstance.TaskExecutionId,
                Error = errorMessage,
                TreatTaskAsFailed = treatTaskAsFailed
            };
            await _taskExecutionRepository.ErrorAsync(request);
        }

        public TExecutionHeader GetHeader<TExecutionHeader>()
        {
            if (_taskExecutionHeader != null)
                return (TExecutionHeader)_taskExecutionHeader;

            return default(TExecutionHeader);
        }

        public ICriticalSectionContext CreateCriticalSection()
        {
            if (!IsExecutionContextActive)
                throw new ExecutionException(NotActiveMessage);

            if (IsUserCriticalSectionActive())
                throw new CriticalSectionException("Only one user critical section context can be active at a time for one context. Check that you are not nesting critical sections with the same context.");

            _userCriticalSectionContext = new CriticalSectionContext(_criticalSectionRepository,
                _taskExecutionInstance,
                _taskExecutionOptions,
                CriticalSectionType.User);

            return _userCriticalSectionContext;
        }

        public async Task<IList<IDateRangeBlockContext>> GetDateRangeBlocksAsync(Func<IFluentDateRangeBlockDescriptor, object> fluentBlockRequest)
        {
            if (!IsExecutionContextActive)
                throw new ExecutionException(NotActiveMessage);

            var fluentDescriptor = fluentBlockRequest(new FluentRangeBlockDescriptor());
            var settings = (IBlockSettings)fluentDescriptor;

            var request = ConvertToDateRangeBlockRequest(settings);
            if (ShouldProtect(request))
            {
                var csContext = CreateClientCriticalSection();
                try
                {
                    var csStarted = await csContext.TryStartAsync(new TimeSpan(0, 0, 20), 3);
                    if (csStarted)
                        return await _blockFactory.GenerateDateRangeBlocksAsync(request);

                    throw new CriticalSectionException("Could not start a critical section in the alloted time");
                }
                finally
                {
                    await csContext.CompleteAsync();
                }
            }
            else
            {
                return await _blockFactory.GenerateDateRangeBlocksAsync(request);
            }
        }

        public async Task<IList<INumericRangeBlockContext>> GetNumericRangeBlocksAsync(Func<IFluentNumericRangeBlockDescriptor, object> fluentBlockRequest)
        {
            if (!IsExecutionContextActive)
                throw new ExecutionException(NotActiveMessage);

            var fluentDescriptor = fluentBlockRequest(new FluentRangeBlockDescriptor());
            var settings = (IBlockSettings)fluentDescriptor;

            var request = ConvertToNumericRangeBlockRequest(settings);
            if (ShouldProtect(request))
            {
                var csContext = CreateClientCriticalSection();
                try
                {
                    var csStarted = await csContext.TryStartAsync(new TimeSpan(0, 0, 20), 3);
                    if (csStarted)
                        return await _blockFactory.GenerateNumericRangeBlocksAsync(request);

                    throw new CriticalSectionException("Could not start a critical section in the alloted time");
                }
                finally
                {
                    await csContext.CompleteAsync();
                }
            }
            else
            {
                return await _blockFactory.GenerateNumericRangeBlocksAsync(request);
            }
        }

        public async Task<IList<IListBlockContext<T>>> GetListBlocksAsync<T>(Func<IFluentListBlockDescriptorBase<T>, object> fluentBlockRequest)
        {
            if (!IsExecutionContextActive)
                throw new ExecutionException(NotActiveMessage);

            var fluentDescriptor = fluentBlockRequest(new FluentListBlockDescriptorBase<T>());
            var settings = (IBlockSettings)fluentDescriptor;

            if (settings.BlockType == BlockType.List)
            {
                var request = ConvertToListBlockRequest(settings);
                if (ShouldProtect(request))
                {
                    var csContext = CreateClientCriticalSection();
                    try
                    {
                        var csStarted = await csContext.TryStartAsync(new TimeSpan(0, 0, 20), 3);
                        if (csStarted)
                            return await _blockFactory.GenerateListBlocksAsync<T>(request);
                        throw new CriticalSectionException("Could not start a critical section in the alloted time");
                    }
                    finally
                    {
                        await csContext.CompleteAsync();
                    }
                }
                else
                {
                    return await _blockFactory.GenerateListBlocksAsync<T>(request);
                }
            }

            throw new NotSupportedException("BlockType not supported");
        }

        public async Task<IList<IListBlockContext<TItem, THeader>>> GetListBlocksAsync<TItem, THeader>(Func<IFluentListBlockDescriptorBase<TItem, THeader>, object> fluentBlockRequest)
        {
            if (!IsExecutionContextActive)
                throw new ExecutionException(NotActiveMessage);

            var fluentDescriptor = fluentBlockRequest(new FluentListBlockDescriptorBase<TItem, THeader>());
            var settings = (IBlockSettings)fluentDescriptor;

            if (settings.BlockType == BlockType.List)
            {
                var request = ConvertToListBlockRequest(settings);
                if (ShouldProtect(request))
                {
                    var csContext = CreateClientCriticalSection();
                    try
                    {
                        var csStarted = await csContext.TryStartAsync(new TimeSpan(0, 0, 20), 3);
                        if (csStarted)
                            return await _blockFactory.GenerateListBlocksAsync<TItem, THeader>(request);
                        throw new CriticalSectionException("Could not start a critical section in the alloted time");
                    }
                    finally
                    {
                        await csContext.CompleteAsync();
                    }
                }
                else
                {
                    return await _blockFactory.GenerateListBlocksAsync<TItem, THeader>(request);
                }
            }

            throw new NotSupportedException("BlockType not supported");
        }

        public async Task<IList<IObjectBlockContext<T>>> GetObjectBlocksAsync<T>(Func<IFluentObjectBlockDescriptorBase<T>, object> fluentBlockRequest)
        {
            if (!IsExecutionContextActive)
                throw new ExecutionException(NotActiveMessage);

            var fluentDescriptor = fluentBlockRequest(new FluentObjectBlockDescriptorBase<T>());
            var settings = (IObjectBlockSettings<T>)fluentDescriptor;

            var request = ConvertToObjectBlockRequest(settings);
            if (ShouldProtect(request))
            {
                var csContext = CreateClientCriticalSection();
                try
                {
                    var csStarted = await csContext.TryStartAsync(new TimeSpan(0, 0, 20), 3);
                    if (csStarted)
                        return await _blockFactory.GenerateObjectBlocksAsync(request);
                    throw new CriticalSectionException("Could not start a critical section in the alloted time");
                }
                finally
                {
                    await csContext.CompleteAsync();
                }
            }
            else
            {
                return await _blockFactory.GenerateObjectBlocksAsync(request);
            }
        }

        public async Task<IDateRangeBlock> GetLastDateRangeBlockAsync(LastBlockOrder lastBlockOrder)
        {
            if (!IsExecutionContextActive)
                throw new ExecutionException(NotActiveMessage);

            var request = new LastBlockRequest(new TaskId(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName),
                BlockType.DateRange);
            request.LastBlockOrder = lastBlockOrder;

            return await _rangeBlockRepository.GetLastRangeBlockAsync(request);
        }

        public async Task<INumericRangeBlock> GetLastNumericRangeBlockAsync(LastBlockOrder lastBlockOrder)
        {
            if (!IsExecutionContextActive)
                throw new ExecutionException(NotActiveMessage);

            var request = new LastBlockRequest(new TaskId(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName),
                BlockType.NumericRange);
            request.LastBlockOrder = lastBlockOrder;

            return await _rangeBlockRepository.GetLastRangeBlockAsync(request);
        }

        public async Task<IListBlock<T>> GetLastListBlockAsync<T>()
        {
            if (!IsExecutionContextActive)
                throw new ExecutionException(NotActiveMessage);

            var request = new LastBlockRequest(new TaskId(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName),
                BlockType.List);

            return await _blockFactory.GetLastListBlockAsync<T>(request);
        }

        public async Task<IListBlock<TItem, THeader>> GetLastListBlockAsync<TItem, THeader>()
        {
            if (!IsExecutionContextActive)
                throw new ExecutionException(NotActiveMessage);

            var request = new LastBlockRequest(new TaskId(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName),
                BlockType.List);

            return await _blockFactory.GetLastListBlockAsync<TItem, THeader>(request);
        }

        public async Task<IObjectBlock<T>> GetLastObjectBlockAsync<T>()
        {
            if (!IsExecutionContextActive)
                throw new ExecutionException(NotActiveMessage);

            var request = new LastBlockRequest(new TaskId(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName),
                BlockType.Object);

            return await _objectBlockRepository.GetLastObjectBlockAsync<T>(request);
        }

        public async Task<TaskExecutionMeta> GetLastExecutionMetaAsync()
        {
            var request = CreateTaskExecutionMetaRequest(1);

            var response = await _taskExecutionRepository.GetLastExecutionMetasAsync(request);
            if (response.Executions != null && response.Executions.Any())
            {
                var meta = response.Executions.First();
                return new TaskExecutionMeta(meta.StartedAt, meta.CompletedAt, meta.Status, meta.ReferenceValue);
            }

            return null;
        }

        public async Task<IList<TaskExecutionMeta>> GetLastExecutionMetasAsync(int numberToRetrieve)
        {
            var request = CreateTaskExecutionMetaRequest(numberToRetrieve);

            var response = await _taskExecutionRepository.GetLastExecutionMetasAsync(request);
            if (response.Executions != null && response.Executions.Any())
            {
                return response.Executions.Select(x => new TaskExecutionMeta(x.StartedAt, x.CompletedAt, x.Status, x.ReferenceValue)).ToList();
            }

            return new List<TaskExecutionMeta>();
        }

        public async Task<TaskExecutionMeta<TExecutionHeader>> GetLastExecutionMetaAsync<TExecutionHeader>()
        {
            var request = CreateTaskExecutionMetaRequest(1);

            var response = await _taskExecutionRepository.GetLastExecutionMetasAsync(request);
            if (response.Executions != null && response.Executions.Any())
            {
                var meta = response.Executions.First();
                return new TaskExecutionMeta<TExecutionHeader>(meta.StartedAt,
                    meta.CompletedAt,
                    meta.Status,
                    JsonGenericSerializer.Deserialize<TExecutionHeader>(meta.Header, true),
                    meta.ReferenceValue);
            }

            return null;
        }

        public async Task<IList<TaskExecutionMeta<TExecutionHeader>>> GetLastExecutionMetasAsync<TExecutionHeader>(int numberToRetrieve)
        {
            var request = CreateTaskExecutionMetaRequest(numberToRetrieve);

            var response = await _taskExecutionRepository.GetLastExecutionMetasAsync(request);
            if (response.Executions != null && response.Executions.Any())
            {
                return response.Executions.Select(x => new TaskExecutionMeta<TExecutionHeader>(x.StartedAt,
                                                    x.CompletedAt,
                                                    x.Status,
                                                    JsonGenericSerializer.Deserialize<TExecutionHeader>(x.Header, true),
                                                    x.ReferenceValue))
                                          .ToList();
            }

            return new List<TaskExecutionMeta<TExecutionHeader>>();
        }

        #endregion .: Public methods :.

        #region .: Private methods :.

        private bool IsExecutionContextActive
        {
            get
            {
                return _startedCalled && !_completeCalled;
            }
        }

        private void CleanUpOldData()
        {
            _cleanUpService.CleanOldData(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName);
        }

        private TaskExecutionStartRequest CreateStartRequest(string referenceValue)
        {
            var startRequest = new TaskExecutionStartRequest(new TaskId(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName),
                _taskExecutionOptions.TaskDeathMode,
                _taskExecutionOptions.ConcurrencyLimit,
                _taskConfiguration.FailedTaskRetryLimit,
                _taskConfiguration.DeadTaskRetryLimit);

            SetStartRequestValues(startRequest, referenceValue);
            SetStartRequestTasklingVersion(startRequest);
            SerializeHeaderIfExists(startRequest);

            return startRequest;
        }

        private void SetStartRequestTasklingVersion(TaskExecutionStartRequest startRequest)
        {
            Assembly assembly = Assembly.GetExecutingAssembly();
            FileVersionInfo fileVersionInfo = FileVersionInfo.GetVersionInfo(assembly.Location);
            string version = fileVersionInfo.ProductVersion;
            startRequest.TasklingVersion = version;
        }

        private void SetStartRequestValues(TaskExecutionStartRequest startRequest, string referenceValue)
        {
            if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                if (!_taskExecutionOptions.KeepAliveInterval.HasValue)
                    throw new ExecutionArgumentsException("KeepAliveInterval must be set when using KeepAlive mode");

                if (!_taskExecutionOptions.KeepAliveDeathThreshold.HasValue)
                    throw new ExecutionArgumentsException("KeepAliveDeathThreshold must be set when using KeepAlive mode");


                startRequest.KeepAliveDeathThreshold = _taskExecutionOptions.KeepAliveDeathThreshold;
                startRequest.KeepAliveInterval = _taskExecutionOptions.KeepAliveInterval;
            }

            if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.Override)
            {
                if (!_taskExecutionOptions.OverrideThreshold.HasValue)
                    throw new ExecutionArgumentsException("OverrideThreshold must be set when using KeepAlive mode");

                startRequest.OverrideThreshold = _taskExecutionOptions.OverrideThreshold.Value;
            }

            startRequest.ReferenceValue = referenceValue;
        }

        private void SerializeHeaderIfExists(TaskExecutionStartRequest startRequest)
        {
            if (_taskExecutionHeader != null)
            {
                startRequest.TaskExecutionHeader = JsonGenericSerializer.Serialize(_taskExecutionHeader);
            }
        }

        private void StartKeepAlive()
        {
            var keepAliveRequest = new SendKeepAliveRequest()
            {
                TaskId = new TaskId(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName),
                TaskExecutionId = _taskExecutionInstance.TaskExecutionId,
                ExecutionTokenId = _taskExecutionInstance.ExecutionTokenId
            };

            _keepAliveDaemon = new KeepAliveDaemon(_taskExecutionRepository, new WeakReference(this));
            _keepAliveDaemon.Run(keepAliveRequest, _taskExecutionOptions.KeepAliveInterval.Value);
        }

        private DateRangeBlockRequest ConvertToDateRangeBlockRequest(IBlockSettings settings)
        {
            var request = new DateRangeBlockRequest();
            request.ApplicationName = _taskExecutionInstance.ApplicationName;
            request.TaskName = _taskExecutionInstance.TaskName;
            request.TaskExecutionId = _taskExecutionInstance.TaskExecutionId;
            request.TaskDeathMode = _taskExecutionOptions.TaskDeathMode;

            if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
                request.KeepAliveDeathThreshold = _taskExecutionOptions.KeepAliveDeathThreshold.Value;
            else
                request.OverrideDeathThreshold = _taskExecutionOptions.OverrideThreshold.Value;

            request.RangeBegin = settings.FromDate;
            request.RangeEnd = settings.ToDate;
            request.MaxBlockRange = settings.MaxBlockTimespan;
            request.ReprocessReferenceValue = settings.ReferenceValueToReprocess;
            request.ReprocessOption = settings.ReprocessOption;

            SetConfigurationOverridableSettings(request, settings);

            return request;
        }

        private NumericRangeBlockRequest ConvertToNumericRangeBlockRequest(IBlockSettings settings)
        {
            var request = new NumericRangeBlockRequest();
            request.ApplicationName = _taskExecutionInstance.ApplicationName;
            request.TaskName = _taskExecutionInstance.TaskName;
            request.TaskExecutionId = _taskExecutionInstance.TaskExecutionId;
            request.TaskDeathMode = _taskExecutionOptions.TaskDeathMode;

            if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
                request.KeepAliveDeathThreshold = _taskExecutionOptions.KeepAliveDeathThreshold.Value;
            else
                request.OverrideDeathThreshold = _taskExecutionOptions.OverrideThreshold.Value;

            request.RangeBegin = settings.FromNumber;
            request.RangeEnd = settings.ToNumber;
            request.BlockSize = settings.MaxBlockNumberRange;
            request.ReprocessReferenceValue = settings.ReferenceValueToReprocess;
            request.ReprocessOption = settings.ReprocessOption;

            SetConfigurationOverridableSettings(request, settings);

            return request;
        }

        private ListBlockRequest ConvertToListBlockRequest(IBlockSettings settings)
        {
            var request = new ListBlockRequest();
            request.ApplicationName = _taskExecutionInstance.ApplicationName;
            request.TaskName = _taskExecutionInstance.TaskName;
            request.TaskExecutionId = _taskExecutionInstance.TaskExecutionId;
            request.TaskDeathMode = _taskExecutionOptions.TaskDeathMode;

            if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
                request.KeepAliveDeathThreshold = _taskExecutionOptions.KeepAliveDeathThreshold.Value;
            else
                request.OverrideDeathThreshold = _taskExecutionOptions.OverrideThreshold.Value;

            request.SerializedValues = settings.Values;
            request.SerializedHeader = settings.Header;
            request.CompressionThreshold = _taskConfiguration.MaxLengthForNonCompressedData;
            request.MaxStatusReasonLength = _taskConfiguration.MaxStatusReason;

            request.MaxBlockSize = settings.MaxBlockSize;
            request.ListUpdateMode = settings.ListUpdateMode;
            request.UncommittedItemsThreshold = settings.UncommittedItemsThreshold;

            request.ReprocessReferenceValue = settings.ReferenceValueToReprocess;
            request.ReprocessOption = settings.ReprocessOption;

            SetConfigurationOverridableSettings(request, settings);


            return request;
        }

        private ObjectBlockRequest<T> ConvertToObjectBlockRequest<T>(IObjectBlockSettings<T> settings)
        {
            var request = new ObjectBlockRequest<T>(settings.Object,
                _taskConfiguration.MaxLengthForNonCompressedData);

            request.ApplicationName = _taskExecutionInstance.ApplicationName;
            request.TaskName = _taskExecutionInstance.TaskName;
            request.TaskExecutionId = _taskExecutionInstance.TaskExecutionId;
            request.TaskDeathMode = _taskExecutionOptions.TaskDeathMode;

            if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
                request.KeepAliveDeathThreshold = _taskExecutionOptions.KeepAliveDeathThreshold.Value;
            else
                request.OverrideDeathThreshold = _taskExecutionOptions.OverrideThreshold.Value;

            request.ReprocessReferenceValue = settings.ReferenceValueToReprocess;
            request.ReprocessOption = settings.ReprocessOption;

            SetConfigurationOverridableSettings(request, settings);

            return request;
        }

        private void SetConfigurationOverridableSettings(BlockRequest request, IBlockSettings settings)
        {
            if (settings.MustReprocessDeadTasks.HasValue)
                request.ReprocessDeadTasks = settings.MustReprocessDeadTasks.Value;
            else
                request.ReprocessDeadTasks = _taskConfiguration.ReprocessDeadTasks;

            if (settings.MustReprocessFailedTasks.HasValue)
                request.ReprocessFailedTasks = settings.MustReprocessFailedTasks.Value;
            else
                request.ReprocessFailedTasks = _taskConfiguration.ReprocessFailedTasks;

            if (settings.DeadTaskRetryLimit.HasValue)
                request.DeadTaskRetryLimit = settings.DeadTaskRetryLimit.Value;
            else
                request.DeadTaskRetryLimit = _taskConfiguration.DeadTaskRetryLimit;

            if (settings.FailedTaskRetryLimit.HasValue)
                request.FailedTaskRetryLimit = settings.FailedTaskRetryLimit.Value;
            else
                request.FailedTaskRetryLimit = _taskConfiguration.FailedTaskRetryLimit;

            if (request.ReprocessDeadTasks)
            {
                if (settings.DeadTaskDetectionRange.HasValue)
                    request.DeadTaskDetectionRange = settings.DeadTaskDetectionRange.Value;
                else
                    request.DeadTaskDetectionRange = _taskConfiguration.ReprocessDeadTasksDetectionRange;
            }

            if (request.ReprocessFailedTasks)
            {
                if (settings.FailedTaskDetectionRange.HasValue)
                    request.FailedTaskDetectionRange = settings.FailedTaskDetectionRange.Value;
                else
                    request.FailedTaskDetectionRange = _taskConfiguration.ReprocessFailedTasksDetectionRange;
            }

            if (settings.MaximumNumberOfBlocksLimit.HasValue)
                request.MaxBlocks = settings.MaximumNumberOfBlocksLimit.Value;
            else
                request.MaxBlocks = _taskConfiguration.MaxBlocksToGenerate;
        }

        private bool IsUserCriticalSectionActive()
        {
            return _userCriticalSectionContext != null && _userCriticalSectionContext.IsActive();
        }

        private bool ShouldProtect(BlockRequest blockRequest)
        {
            return (blockRequest.ReprocessDeadTasks || blockRequest.ReprocessFailedTasks) && !IsUserCriticalSectionActive();
        }

        private ICriticalSectionContext CreateClientCriticalSection()
        {
            if (IsClientCriticalSectionActive())
                throw new CriticalSectionException("Only one client critical section context can be active at a time");

            _clientCriticalSectionContext = new CriticalSectionContext(_criticalSectionRepository,
                _taskExecutionInstance,
                _taskExecutionOptions,
                CriticalSectionType.Client);

            return _clientCriticalSectionContext;
        }

        private bool IsClientCriticalSectionActive()
        {
            return _clientCriticalSectionContext != null && _clientCriticalSectionContext.IsActive();
        }

        private TaskExecutionMetaRequest CreateTaskExecutionMetaRequest(int numberToRetrieve)
        {
            var request = new TaskExecutionMetaRequest();
            request.TaskId = new TaskId(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName);
            request.ExecutionsToRetrieve = numberToRetrieve;

            return request;
        }

        #endregion .: Private methods :.
    }
}
