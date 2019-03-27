using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Taskling.Contexts
{
    public interface ICriticalSectionContext : IDisposable
    {
        bool IsActive();
        Task<bool> TryStartAsync();
        Task<bool> TryStartAsync(TimeSpan retryInterval, int numberOfAttempts);
        Task CompleteAsync();
    }
}
