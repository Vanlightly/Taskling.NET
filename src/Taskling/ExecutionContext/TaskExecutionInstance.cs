using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.ExecutionContext
{
    public class TaskExecutionInstance
    {
        public TaskExecutionInstance()
        {}

        public int TaskExecutionId { get; set; }
        public string ApplicationName { get; set; }
        public string TaskName { get; set; }
        public DateTime StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public Guid ExecutionTokenId { get; set; }
    }
}
