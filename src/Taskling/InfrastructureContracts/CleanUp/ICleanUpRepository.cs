using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Taskling.InfrastructureContracts.CleanUp
{
    public interface ICleanUpRepository
    {
        Task CleanOldDataAsync(CleanUpRequest cleanUpRequest);
    }
}
