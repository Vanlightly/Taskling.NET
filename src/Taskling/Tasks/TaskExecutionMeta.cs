using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Tasks
{
    public class TaskExecutionMeta
    {
        public TaskExecutionMeta(DateTime startedAt,
            DateTime? completedAt,
            TaskExecutionStatus status,
            string referenceValue = null)
        {
            StartedAt = startedAt;
            CompletedAt = completedAt;
            Status = status;
            ReferenceValue = referenceValue;
        }

        public DateTime StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public TaskExecutionStatus Status { get; set; }
        public string ReferenceValue { get; set; }
    }

    public class TaskExecutionMeta<TaskExecutionHeader>
    {
        public TaskExecutionMeta(DateTime startedAt,
            DateTime? completedAt,
            TaskExecutionStatus status,
            TaskExecutionHeader header,
            string referenceValue = null)
        {
            StartedAt = startedAt;
            CompletedAt = completedAt;
            Status = status;
            Header = header;
            ReferenceValue = referenceValue;
        }

        public DateTime StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public TaskExecutionStatus Status { get; set; }
        public TaskExecutionHeader Header { get; set; }
        public string ReferenceValue { get; set; }
    }
}
