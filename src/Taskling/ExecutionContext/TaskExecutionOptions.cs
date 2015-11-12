using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.ExecutionContext
{
    public enum TaskDeathMode
    {
        KeepAlive,
        OverrideAfterElapsedTimePeriodFromGrantDate
    }

    public class TaskExecutionOptions
    {
        public TaskDeathMode TaskDeathMode { get; set; }
        public int? SecondsOverride { get; set; }
        public TimeSpan? KeepAliveInterval { get; set; }
        public TimeSpan? KeepAliveElapsed { get; set; }
    }
}
