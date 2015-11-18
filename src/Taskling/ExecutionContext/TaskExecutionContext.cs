using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.Client;
using Taskling.CriticalSection;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.ExecutionContext
{
    public class TaskExecutionContext : ITaskExecutionContext
    {
        private readonly ITaskExecutionService _taskExecutionService;
        private readonly ICriticalSectionService _criticalSectionService;

        private delegate void KeepAliveDelegate();
        private TaskExecutionInstance _taskExecutionInstance;
        private TaskExecutionOptions _taskExecutionOptions;
        private bool _startedCalled;
        private bool _completeCalled;

        public TaskExecutionContext(ITaskExecutionService taskExecutionService,
            ICriticalSectionService criticalSectionService,
            string applicationName,
            string taskName,
            TaskExecutionOptions taskExecutionOptions)
        {
            _taskExecutionService = taskExecutionService;
            _criticalSectionService = criticalSectionService;

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

                if (response.GrantStatus == GrantStatus.GrantedWithoutLimit)
                    _taskExecutionInstance.UnlimitedMode = true;
                else
                    _taskExecutionInstance.UnlimitedMode = false;

                if (response.GrantStatus == GrantStatus.Denied)
                {
                    Complete();
                    return false;
                }
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

            var completeRequest = new TaskExecutionCompleteRequest(_taskExecutionInstance.ApplicationName,
                _taskExecutionInstance.TaskName,
                _taskExecutionInstance.TaskExecutionId,
                _taskExecutionInstance.ExecutionTokenId);

            completeRequest.UnlimitedMode = _taskExecutionInstance.UnlimitedMode;

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
                _taskExecutionOptions.TaskDeathMode,
                _taskExecutionOptions.SecondsOverride
                );

            if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
            {
                if (!_taskExecutionOptions.KeepAliveElapsed.HasValue)
                    throw new ExecutionArgumentsException("KeepAliveElapsed must be set when using KeepAlive mode");

                StartKeepAlive();
                startRequest.KeepAliveElapsedSeconds = (int)_taskExecutionOptions.KeepAliveElapsed.Value.TotalSeconds;
            }

            return startRequest;
        }

        #region .: Keep Alive :.

        private void StartKeepAlive()
        {
            var sendDelegate = new KeepAliveDelegate(RunKeepAlive);
            sendDelegate.BeginInvoke(new AsyncCallback(KeepAliveCallback), sendDelegate);
        }

        private void KeepAliveCallback(IAsyncResult ar)
        {
            try
            {
                var caller = (KeepAliveDelegate)ar.AsyncState;
                caller.EndInvoke(ar);
            }
            catch (Exception)
            { }
        }

        private void RunKeepAlive()
        {
            if (_startedCalled)
            {
                DateTime lastKeepAlive = DateTime.UtcNow;
                _taskExecutionService.SendKeepAlive(_taskExecutionInstance.TaskExecutionId);

                while (!_completeCalled)
                {
                    var timespanSinceLastKeepAlive = DateTime.UtcNow - lastKeepAlive;
                    if (timespanSinceLastKeepAlive > _taskExecutionOptions.KeepAliveInterval)
                    {
                        lastKeepAlive = DateTime.UtcNow;
                        _taskExecutionService.SendKeepAlive(_taskExecutionInstance.TaskExecutionId);
                    }
                    Thread.Sleep(1000);
                }
            }
        }

        #endregion .: Keep Alive :.
    }
}
