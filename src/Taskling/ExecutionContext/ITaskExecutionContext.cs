using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.CriticalSectionContext;

namespace Taskling.ExecutionContext
{
    public interface ITaskExecutionContext : IDisposable
    {
        bool TryStart();
        void Complete();
        void Checkpoint(string checkpointMessage);
        void Error(string errorMessage);
        ICriticalSectionContext StartCriticalSection();
    }
}
