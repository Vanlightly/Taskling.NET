using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.CriticalSections;

namespace Taskling.InfrastructureContracts
{
    public interface ICriticalSectionService
    {
        StartCriticalSectionResponse Start(StartCriticalSectionRequest startRequest);
        CompleteCriticalSectionResponse Complete(CompleteCriticalSectionRequest completeCriticalSectionRequest);
    }
}
