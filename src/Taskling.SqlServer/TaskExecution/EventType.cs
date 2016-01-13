using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.TaskExecution
{
    enum EventType
    {
        Start = 0,
        CheckPoint = 1,
        Error = 2,
        End = 3
    }
}
