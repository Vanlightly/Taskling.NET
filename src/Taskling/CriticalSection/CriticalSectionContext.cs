using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.Exceptions;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CriticalSections;

namespace Taskling.CriticalSection
{
    public class CriticalSectionContext : ICriticalSectionContext
    {
        private readonly ICriticalSectionService _criticalSectionService;
        private readonly TaskExecutionInstance _taskExecutionInstance;
        private readonly TaskExecutionOptions _taskExecutionOptions;

        private bool _started;
        private bool _completeCalled;

        public CriticalSectionContext(ICriticalSectionService criticalSectionService, 
            TaskExecutionInstance taskExecutionInstance,
            TaskExecutionOptions taskExecutionOptions)
        {
            _criticalSectionService = criticalSectionService;
            _taskExecutionInstance = taskExecutionInstance;
            _taskExecutionOptions = taskExecutionOptions;

            ValidateOptions();
        }

        public bool TryStart()
        {
            if(_started)
                throw new ExecutionException("There is already an active critical section");

            _started = true;

            var startRequest = new StartCriticalSectionRequest(_taskExecutionInstance.ApplicationName,
                _taskExecutionInstance.TaskName,
                _taskExecutionInstance.TaskExecutionId,
                _taskExecutionOptions.TaskDeathMode);

            if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate)
                startRequest.SecondsOverride = _taskExecutionOptions.SecondsOverride.Value;

            if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
                startRequest.KeepAliveElapsedSeconds = (int)_taskExecutionOptions.KeepAliveElapsed.Value.TotalSeconds;

            var response = _criticalSectionService.Start(startRequest);
            if (response.GrantStatus == GrantStatus.Denied)
            {
                _started = false;
                return false;
            }

            return true;
        }

        public bool TryStart(TimeSpan retryInterval, int numberOfAttempts)
        {
            int tryCount = 0;
            bool started = false;

            while (started == false && tryCount <= numberOfAttempts)
            {
                tryCount++;
                started = TryStart();
                if(!started)
                    Thread.Sleep(retryInterval);
            }

            return started;
        }

        public void Complete()
        {
            if(!_started)
                throw new ExecutionException("There is no active critical section to complete");

            var completeRequest = new CompleteCriticalSectionRequest(_taskExecutionInstance.ApplicationName,
                _taskExecutionInstance.TaskName,
                _taskExecutionInstance.TaskExecutionId);

            _criticalSectionService.Complete(completeRequest);

            _completeCalled = true;
        }

        public void Dispose()
        {
            if(_started && !_completeCalled)
                Complete();
        }

        private void ValidateOptions()
        {
            if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                if(!_taskExecutionOptions.KeepAliveElapsed.HasValue)
                    throw new ExecutionArgumentsException("KeepAliveElapsed must be set when using KeepAlive mode");
                
                if (!_taskExecutionOptions.KeepAliveInterval.HasValue)
                    throw new ExecutionArgumentsException("KeepAliveInterval must be set when using KeepAlive mode");
            }
            else if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate)
            {
                if (!_taskExecutionOptions.SecondsOverride.HasValue)
                    throw new ExecutionArgumentsException("SecondsOverride must be set when using Override mode");
            }
        }
    }
}
