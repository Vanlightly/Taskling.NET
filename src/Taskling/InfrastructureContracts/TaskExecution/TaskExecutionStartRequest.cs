using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Tasks;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public class TaskExecutionStartRequest : RequestBase
    {
        public TaskExecutionStartRequest(TaskId taskId,
            TaskDeathMode taskDeathMode,
            int concurrencyLimit,
            short failedTaskRetryLimit,
            short deadTaskRetryLimit
            )
            : base(taskId)
        {
            TaskDeathMode = taskDeathMode;
            ConcurrencyLimit = concurrencyLimit;
            FailedTaskRetryLimit = failedTaskRetryLimit;
            DeadTaskRetryLimit = deadTaskRetryLimit;
        }

        public string TasklingVersion { get; set; }
        public TaskDeathMode TaskDeathMode { get; set; }
        public TimeSpan? OverrideThreshold { get; set; }
        public TimeSpan? KeepAliveInterval { get; set; }
        public TimeSpan? KeepAliveDeathThreshold { get; set; }
        public string ReferenceValue { get; set; }
        public int ConcurrencyLimit { get; set; }
        public short FailedTaskRetryLimit { get; set; }
        public short DeadTaskRetryLimit { get; set; }
        public string TaskExecutionHeader { get; set; }
    }
}
