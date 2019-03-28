using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Events;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Events;

namespace Taskling.SqlServer.Events
{
    public class EventsRepository : DbOperationsService, IEventsRepository
    {
        public async Task LogEventAsync(TaskId taskId, string taskExecutionId, EventType eventType, string message)
        {
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                using (var command = new SqlCommand(EventsQueryBuilder.InsertTaskExecutionEventQuery, connection))
                {
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                    command.Parameters.Add(new SqlParameter("@TaskExecutionId", SqlDbType.Int)).Value = int.Parse(taskExecutionId);
                    command.Parameters.Add(new SqlParameter("@EventType", SqlDbType.Int)).Value = (int)eventType;

                    if (message == null)
                    {
                        command.Parameters.Add(new SqlParameter("@Message", SqlDbType.NVarChar, -1)).Value = DBNull.Value;
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@Message", SqlDbType.NVarChar, -1)).Value = message;
                    }

                    command.Parameters.Add(new SqlParameter("@EventDateTime", SqlDbType.DateTime)).Value = DateTime.UtcNow;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }
    }
}
