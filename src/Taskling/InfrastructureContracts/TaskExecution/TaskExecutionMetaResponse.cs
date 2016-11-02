using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public class TaskExecutionMetaResponse
    {
        public TaskExecutionMetaResponse()
        {
            Executions = new List<TaskExecutionMetaItem>();
        }

        public List<TaskExecutionMetaItem> Executions { get; set; }
    }
}
