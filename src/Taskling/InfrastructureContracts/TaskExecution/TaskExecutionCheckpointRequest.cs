using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public class TaskExecutionCheckpointRequest : RequestBase
    {
        public string Message { get; set; }
    }
}
