using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.CriticalSection
{
    public interface ICriticalSection
    {
        void Start();
        void Complete();
    }
}
