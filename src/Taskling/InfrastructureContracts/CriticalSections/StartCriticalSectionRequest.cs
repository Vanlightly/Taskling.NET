using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.CriticalSections
{
    public class StartCriticalSectionRequest : RequestBase
    {
        public int SecondsOverride { get; set; }
    }
}
