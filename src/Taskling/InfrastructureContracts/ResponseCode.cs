using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public enum ResponseCode
    {
        Ok,
        Failed,
        FailedWithPotentialDataConsistencyIssue
    }
}
