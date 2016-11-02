using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.CleanUp
{
    public interface ICleanUpService
    {
        void CleanOldData(string applicationName, string taskName);
    }
}
