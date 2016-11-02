using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Events
{
    public enum EventType
    {
        NotDefined = 0,
        Start = 1,
        CheckPoint = 2,
        Error = 3,
        End = 4,
        Blocked = 5
    }
}
