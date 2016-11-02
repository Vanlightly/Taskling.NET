using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.TaskExecution
{
    public interface ITaskRepository
    {
        TaskDefinition EnsureTaskDefinition(TaskId taskId);
        DateTime GetLastTaskCleanUpTime(TaskId taskId);
        void SetLastCleaned(TaskId taskId);
    }
}
