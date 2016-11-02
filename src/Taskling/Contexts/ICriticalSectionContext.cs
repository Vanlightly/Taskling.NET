using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Contexts
{
    public interface ICriticalSectionContext : IDisposable
    {
        bool IsActive();
        bool TryStart();
        bool TryStart(TimeSpan retryInterval, int numberOfAttempts);
        void Complete();
    }
}
