using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.Blocks
{
    public enum BlockExecutionStatus
    {
        NotStarted,
        Started,
        Completed,
        Failed
    }
}
