using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.CriticalSections
{
    public interface ICriticalSectionRepository
    {
        StartCriticalSectionResponse Start(StartCriticalSectionRequest startRequest);
        CompleteCriticalSectionResponse Complete(CompleteCriticalSectionRequest completeCriticalSectionRequest);
    }
}
