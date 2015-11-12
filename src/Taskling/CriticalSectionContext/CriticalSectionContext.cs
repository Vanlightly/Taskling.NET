using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts;

namespace Taskling.CriticalSectionContext
{
    public class CriticalSectionContext : ICriticalSectionContext
    {
        private readonly ICriticalSectionService _criticalSectionService;
        private readonly TaskExecutionInstance _taskExecutionInstance;

        public CriticalSectionContext(ICriticalSectionService criticalSectionService, TaskExecutionInstance taskExecutionInstance)
        {
            _criticalSectionService = criticalSectionService;
            _taskExecutionInstance = taskExecutionInstance;
        }

        public void Complete()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
