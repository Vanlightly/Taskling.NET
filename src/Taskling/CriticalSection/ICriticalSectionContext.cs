using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.CriticalSection
{
    public interface ICriticalSectionContext : IDisposable
    {
        bool TryStart();
        bool TryStart(TimeSpan retryInterval, int numberOfAttempts);
        void Complete();
    }
}
