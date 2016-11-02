using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Tasks
{
    public class TaskExecutionOptions
    {
        public TaskDeathMode TaskDeathMode { get; set; }
        public TimeSpan? OverrideThreshold { get; set; }
        public TimeSpan? KeepAliveInterval { get; set; }
        public TimeSpan? KeepAliveDeathThreshold { get; set; }
        public int ConcurrencyLimit { get; set; }
        public bool Enabled { get; set; }
    }
}
