using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public class TaskExecutionCompleteResponse : ResponseBase
    {
        public DateTime CompletedAt { get; set; }
    }
}
