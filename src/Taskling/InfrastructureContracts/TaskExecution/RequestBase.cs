using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public class RequestBase
    {
        public RequestBase()
        { }

        public RequestBase(TaskId taskId)
        {
            TaskId = taskId;
        }

        public RequestBase(TaskId taskId, string taskExecutionId)
        {
            TaskId = taskId;
            TaskExecutionId = taskExecutionId;
        }

        public TaskId TaskId { get; set; }
        public string TaskExecutionId { get; set; }
    }
}
