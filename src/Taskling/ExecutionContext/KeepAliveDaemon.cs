using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.ExecutionContext
{
    internal class KeepAliveDaemon
    {
        private delegate void KeepAliveDelegate(string taskExecutionId, TimeSpan keepAliveInterval);
        private WeakReference _owner;
        private readonly ITaskExecutionService _taskExecutionService;
        private bool _completeCalled;

        public KeepAliveDaemon(ITaskExecutionService taskExecutionService, WeakReference owner)
        {
            _owner = owner;
            _taskExecutionService = taskExecutionService;
        }

        public void Stop()
        {
            _completeCalled = true;
        }

        public void Run(string taskExecutionId, TimeSpan keepAliveInterval)
        {
            var sendDelegate = new KeepAliveDelegate(StartKeepAlive);
            sendDelegate.BeginInvoke(taskExecutionId, keepAliveInterval, new AsyncCallback(KeepAliveCallback), sendDelegate);
        }

        private void StartKeepAlive(string taskExecutionId, TimeSpan keepAliveInterval)
        {
            DateTime lastKeepAlive = DateTime.UtcNow;
            _taskExecutionService.SendKeepAlive(taskExecutionId);

            while (!_completeCalled && _owner.IsAlive)
            {
                var timespanSinceLastKeepAlive = DateTime.UtcNow - lastKeepAlive;
                if (timespanSinceLastKeepAlive > keepAliveInterval)
                {
                    lastKeepAlive = DateTime.UtcNow;
                    _taskExecutionService.SendKeepAlive(taskExecutionId);
                }
                Thread.Sleep(1000);
            }
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
    }
}
