using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Tasks;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public class TaskExecutionMetaItem
    {
        public DateTime StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public TaskExecutionStatus Status { get; set; }
        public string Header { get; set; }
        public string ReferenceValue { get; set; }
    }
}
