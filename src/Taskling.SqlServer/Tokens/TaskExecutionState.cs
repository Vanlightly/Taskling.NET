using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Tasks;

namespace Taskling.SqlServer.Tokens
{
    public class TaskExecutionState
    {
        public string TaskExecutionId { get; set; }
        public DateTime StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public DateTime? LastKeepAlive { get; set; }
        public TaskDeathMode TaskDeathMode { get; set; }
        public TimeSpan? OverrideThreshold { get; set; }
        public TimeSpan? KeepAliveInterval { get; set; }
        public TimeSpan? KeepAliveDeathThreshold { get; set; }
        public DateTime CurrentDateTime { get; set; }
        public int QueueIndex { get; set; }
    }
}
