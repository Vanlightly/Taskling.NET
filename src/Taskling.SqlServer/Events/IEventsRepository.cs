using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Events;
using Taskling.InfrastructureContracts;

namespace Taskling.SqlServer.Events
{
    public interface IEventsRepository
    {
        void LogEvent(TaskId taskId, string taskExecutionId, EventType eventType, string message);
    }
}
