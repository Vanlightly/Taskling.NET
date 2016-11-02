using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Exceptions
{
    [Serializable]
    public class CriticalSectionException : Exception
    {
        public CriticalSectionException(string message)
            : base(message)
        { }
    }
}
