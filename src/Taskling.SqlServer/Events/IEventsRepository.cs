using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Events;
using Taskling.InfrastructureContracts;

namespace Taskling.SqlServer.Events
{
    public interface IEventsRepository
    {
        Task LogEventAsync(TaskId taskId, string taskExecutionId, EventType eventType, string message);
    }
}
