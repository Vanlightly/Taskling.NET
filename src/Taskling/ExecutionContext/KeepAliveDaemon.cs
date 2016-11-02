using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.ExecutionContext
{
    internal class KeepAliveDaemon
    {
        private delegate void KeepAliveDelegate(SendKeepAliveRequest sendKeepAliveRequest, TimeSpan keepAliveInterval);
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
            var sendDelegate = new KeepAliveDelegate(StartKeepAlive);
            sendDelegate.BeginInvoke(sendKeepAliveRequest, keepAliveInterval, new AsyncCallback(KeepAliveCallback), sendDelegate);
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
