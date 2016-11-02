using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.SqlServer.Tasks
{
    internal class CachedTaskDefinition
    {
        public TaskDefinition TaskDefinition { get; set; }
        public DateTime CachedAt { get; set; }
    }
}
