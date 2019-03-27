using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Taskling.Contexts
{
    public interface IBlockContext
    {
        Task StartAsync();
        Task CompleteAsync();
        Task FailedAsync();
        Task FailedAsync(string message);
    }
}
