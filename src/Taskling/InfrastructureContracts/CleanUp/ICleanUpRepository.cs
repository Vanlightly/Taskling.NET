using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.CleanUp
{
    public interface ICleanUpRepository
    {
        void CleanOldData(CleanUpRequest cleanUpRequest);
    }
}
