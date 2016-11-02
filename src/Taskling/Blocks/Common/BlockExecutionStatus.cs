using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks.Common
{
    public enum BlockExecutionStatus
    {
        NotDefined = 0,
        NotStarted = 1,
        Started = 2,
        Completed = 3,
        Failed = 4
    }
}
