using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.CriticalSection;

namespace Taskling.ExecutionContext
{
    public interface ITaskExecutionContext : IDisposable
    {
        bool TryStart();
        void Complete();
        void Checkpoint(string checkpointMessage);
        void Error(string errorMessage);
        ICriticalSectionContext CreateCriticalSection();
    }
}
