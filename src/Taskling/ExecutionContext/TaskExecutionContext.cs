using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.Blocks;
using Taskling.Blocks.Requests;
using Taskling.Client;
using Taskling.CriticalSection;
using Taskling.Exceptions;
using Taskling.ExecutionContext.FluentBlocks;
using Taskling.ExecutionContext.FluentBlocks.List;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.ExecutionContext
{
    public class TaskExecutionContext : ITaskExecutionContext
    {
        private readonly ITaskExecutionService _taskExecutionService;
        private readonly ICriticalSectionService _criticalSectionService;
        private readonly IBlockFactory _blockFactory;

        private TaskExecutionInstance _taskExecutionInstance;
        private TaskExecutionOptions _taskExecutionOptions;
        private bool _startedCalled;
        private bool _completeCalled;
        private KeepAliveDaemon _keepAliveDaemon;

        public TaskExecutionContext(ITaskExecutionService taskExecutionService,
            ICriticalSectionService criticalSectionService,
            IBlockFactory blockFactory,
            string applicationName,
            string taskName,
            TaskExecutionOptions taskExecutionOptions)
        {
            _taskExecutionService = taskExecutionService;
            _criticalSectionService = criticalSectionService;
            _blockFactory = blockFactory;

            _taskExecutionInstance = new TaskExecutionInstance();
            _taskExecutionInstance.ApplicationName = applicationName;
            _taskExecutionInstance.TaskName = taskName;

            _taskExecutionOptions = taskExecutionOptions;
        }

        public bool TryStart()
        {
            if (_startedCalled)
                throw new Exception("The execution context has already been started");

            _startedCalled = true;

            var startRequest = CreateStartRequest();

            try
            {
                var response = _taskExecutionService.Start(startRequest);
                _taskExecutionInstance.TaskExecutionId = response.TaskExecutionId;
                _taskExecutionInstance.ExecutionTokenId = response.ExecutionTokenId;

                if (response.GrantStatus == GrantStatus.Denied)
                {
                    Complete();
                    return false;
                }

                if(_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
                    StartKeepAlive();
            }
            catch (Exception)
            {
                _completeCalled = true;
                throw;
            }

            return true;
        }

        public void Complete()
        {
            if (!_startedCalled)
                throw new Exception("This context has not been started yet");

            _completeCalled = true;
            
            if(_keepAliveDaemon != null)
                _keepAliveDaemon.Stop();

            var completeRequest = new TaskExecutionCompleteRequest(_taskExecutionInstance.ApplicationName,
                _taskExecutionInstance.TaskName,
                _taskExecutionInstance.TaskExecutionId,
                _taskExecutionInstance.ExecutionTokenId);

            var response = _taskExecutionService.Complete(completeRequest);
            _taskExecutionInstance.CompletedAt = response.CompletedAt;
        }

        public void Checkpoint(string checkpointMessage)
        {
            throw new NotImplementedException();
        }

        public void Error(string errorMessage)
        {
            throw new NotImplementedException();
        }

        public ICriticalSectionContext CreateCriticalSection()
        {
            var criticalSectionContext = new CriticalSectionContext(_criticalSectionService,
                _taskExecutionInstance,
                _taskExecutionOptions);

            return criticalSectionContext;
        }

        public IList<IRangeBlockContext> GetRangeBlocks(Func<FluentRangeBlockDescriptor, IFluentBlockSettingsDescriptor> fluentBlockRequest)
        {
            var fluentDescriptor = fluentBlockRequest(new FluentRangeBlockDescriptor());
            var settings = (IBlockSettings) fluentDescriptor;

            if (settings.BlockType == BlockType.DateRange)
            {
                var request = ConvertToDateRangeBlockRequest(settings);
                return _blockFactory.GenerateDateRangeBlocks(request);
            }
            
            if (settings.BlockType == BlockType.NumericRange)
            {
                var request = ConvertToNumericRangeBlockRequest(settings);
                return _blockFactory.GenerateNumericRangeBlocks(request);
            }

            throw new NotSupportedException("BlockType not supported");
        }

        public IList<IListBlockContext> GetListBlocks(Func<FluentListBlockDescriptorBase, IFluentBlockSettingsDescriptor> fluentBlockRequest)
        {
            var fluentDescriptor = fluentBlockRequest(new FluentListBlockDescriptorBase());
            var settings = (IBlockSettings)fluentDescriptor;

            if (settings.BlockType == BlockType.List)
            {
                var request = ConvertToListBlockRequest(settings);
                return _blockFactory.GenerateListBlocks(request);
            }

            throw new NotSupportedException("BlockType not supported");
        }

        public void Dispose()
        {
            if (_startedCalled && !_completeCalled)
            {
                Complete();
            }
        }

        private TaskExecutionStartRequest CreateStartRequest()
        {
            var startRequest = new TaskExecutionStartRequest(_taskExecutionInstance.ApplicationName,
                _taskExecutionInstance.TaskName,
                _taskExecutionOptions.TaskDeathMode);

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

            return startRequest;
        }

        private void StartKeepAlive()
        {
            var keepAliveRequest = new SendKeepAliveRequest()
            {
                ApplicationName = _taskExecutionInstance.ApplicationName,
                TaskName = _taskExecutionInstance.TaskName,
                TaskExecutionId = _taskExecutionInstance.TaskExecutionId,
                ExecutionTokenId = _taskExecutionInstance.ExecutionTokenId
            };

            _keepAliveDaemon = new KeepAliveDaemon(_taskExecutionService, new WeakReference(this));
            _keepAliveDaemon.Run(keepAliveRequest, _taskExecutionOptions.KeepAliveInterval.Value);
        }
        
        private DateRangeBlockRequest ConvertToDateRangeBlockRequest(IBlockSettings settings)
        {
            var request = new DateRangeBlockRequest();
            request.ApplicationName = _taskExecutionInstance.ApplicationName;
            request.TaskName = _taskExecutionInstance.TaskName;
            request.TaskExecutionId = _taskExecutionInstance.TaskExecutionId;
            request.CheckForDeadExecutions = settings.MustReprocessDeadTasks;
            request.CheckForFailedExecutions = settings.MustReprocessFailedTasks;
            
            if (settings.MustReprocessDeadTasks)
                request.GoBackTimePeriodForDeadTasks = settings.DeadTaskDetectionRange;
            
            if(settings.MustReprocessFailedTasks)
                request.GoBackTimePeriodForFailedTasks = settings.DeadTaskDetectionRange;

            request.TaskDeathMode = _taskExecutionOptions.TaskDeathMode;

            if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
                request.KeepAliveDeathThreshold = settings.TreatAsDeadAfterRange;
            else
                request.OverrideDeathThreshold = settings.TreatAsDeadAfterRange;

            request.RangeBegin = settings.FromDate;
            request.RangeEnd = settings.ToDate;
            request.MaxBlockRange = settings.MaxBlockTimespan;
            request.MaxBlocks = settings.MaximumNumberOfBlocksLimit;
            
            return request;
        }

        private NumericRangeBlockRequest ConvertToNumericRangeBlockRequest(IBlockSettings settings)
        {
            var request = new NumericRangeBlockRequest();
            request.ApplicationName = _taskExecutionInstance.ApplicationName;
            request.TaskName = _taskExecutionInstance.TaskName;
            request.TaskExecutionId = _taskExecutionInstance.TaskExecutionId;
            request.CheckForDeadExecutions = settings.MustReprocessDeadTasks;
            request.CheckForFailedExecutions = settings.MustReprocessFailedTasks;

            if (settings.MustReprocessDeadTasks)
                request.GoBackTimePeriodForDeadTasks = settings.DeadTaskDetectionRange;

            if (settings.MustReprocessFailedTasks)
                request.GoBackTimePeriodForFailedTasks = settings.DeadTaskDetectionRange;

            request.TaskDeathMode = _taskExecutionOptions.TaskDeathMode;

            if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
                request.KeepAliveDeathThreshold = settings.TreatAsDeadAfterRange;
            else
                request.OverrideDeathThreshold = settings.TreatAsDeadAfterRange;

            request.RangeBegin = settings.FromNumber;
            request.RangeEnd = settings.ToNumber;
            request.BlockSize = settings.MaxBlockNumberRange;
            request.MaxBlocks = settings.MaximumNumberOfBlocksLimit;

            return request;
        }

        private ListBlockRequest ConvertToListBlockRequest(IBlockSettings settings)
        {
            var request = new ListBlockRequest();
            request.ApplicationName = _taskExecutionInstance.ApplicationName;
            request.TaskName = _taskExecutionInstance.TaskName;
            request.TaskExecutionId = _taskExecutionInstance.TaskExecutionId;
            request.CheckForDeadExecutions = settings.MustReprocessDeadTasks;
            request.CheckForFailedExecutions = settings.MustReprocessFailedTasks;

            if (settings.MustReprocessDeadTasks)
                request.GoBackTimePeriodForDeadTasks = settings.DeadTaskDetectionRange;

            if (settings.MustReprocessFailedTasks)
                request.GoBackTimePeriodForFailedTasks = settings.DeadTaskDetectionRange;

            request.TaskDeathMode = _taskExecutionOptions.TaskDeathMode;

            if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
                request.KeepAliveDeathThreshold = settings.TreatAsDeadAfterRange;
            else
                request.OverrideDeathThreshold = settings.TreatAsDeadAfterRange;

            request.Values = settings.Values;
            request.MaxBlockSize = settings.MaxBlockSize;
            request.MaxBlocks = settings.MaximumNumberOfBlocksLimit;
            request.ListUpdateMode = settings.ListUpdateMode;
            request.UncommittedItemsThreshold = settings.UncommittedItemsThreshold;

            return request;
        }
    }
}
