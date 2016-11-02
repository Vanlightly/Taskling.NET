using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Tasks
{
    public enum TaskExecutionStatus
    {
        NotDefined = 0,
        Completed = 1,
        InProgress = 2,
        Dead = 3,
        Failed = 4,
        Blocked = 5
    }
}
