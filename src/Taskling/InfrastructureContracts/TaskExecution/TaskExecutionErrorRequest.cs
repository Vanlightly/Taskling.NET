using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public class TaskExecutionErrorRequest : RequestBase
    {
        public string Error { get; set; }
    }
}
