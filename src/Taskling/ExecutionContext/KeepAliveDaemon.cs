using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.ExecutionContext
{
    internal class KeepAliveDaemon
    {
        private WeakReference _owner;
        private readonly ITaskExecutionRepository _taskExecutionRepository;
        private bool _completeCalled;

        public KeepAliveDaemon(ITaskExecutionRepository taskExecutionRepository, WeakReference owner)
        {
            _owner = owner;
            _taskExecutionRepository = taskExecutionRepository;
        }

        public void Stop()
        {
            _completeCalled = true;
        }

        public void Run(SendKeepAliveRequest sendKeepAliveRequest, TimeSpan keepAliveInterval)
        {
            Task.Run(() => StartKeepAlive(sendKeepAliveRequest, keepAliveInterval));
        }

        private void StartKeepAlive(SendKeepAliveRequest sendKeepAliveRequest, TimeSpan keepAliveInterval)
        {
            DateTime lastKeepAlive = DateTime.UtcNow;
            _taskExecutionRepository.SendKeepAlive(sendKeepAliveRequest);

            while (!_completeCalled && _owner.IsAlive)
            {
                var timespanSinceLastKeepAlive = DateTime.UtcNow - lastKeepAlive;
                if (timespanSinceLastKeepAlive > keepAliveInterval)
                {
                    lastKeepAlive = DateTime.UtcNow;
                    _taskExecutionRepository.SendKeepAlive(sendKeepAliveRequest);
                }
                Thread.Sleep(1000);
            }
        }
    }
}
