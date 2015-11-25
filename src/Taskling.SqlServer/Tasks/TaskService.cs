using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.DataObjects;
using Taskling.SqlServer.TaskExecution;

namespace Taskling.SqlServer.Tasks
{
    public class TaskService : ITaskService
    {
        private readonly string _connectionString;
        private readonly int _queryTimeout;
        private readonly string _tableSchema;

        private object _myCacheSyncObj = new object();
        private Dictionary<string, CachedTaskDefinition> _cachedTaskDefinitions = new Dictionary<string, CachedTaskDefinition>();

        public TaskService(SqlServerClientConnectionSettings clientConnectionSettings)
        {
            _connectionString = clientConnectionSettings.ConnectionString;
            _queryTimeout = (int)clientConnectionSettings.QueryTimeout.TotalMilliseconds;
            _tableSchema = clientConnectionSettings.TableSchema;
        }

        public TaskDefinition GetTaskDefinition(string applicationName, string taskName)
        {
            var taskDefinition = GetTask(applicationName, taskName);
            if (taskDefinition != null)
            {
                return taskDefinition;
            }
            else
            {
                return InsertNewTask(applicationName, taskName);
            }
        }

        private TaskDefinition GetTask(string applicationName, string taskName)
        {
            string key = applicationName + "::" + taskName;
            var cachedTaskDefinition = GetCachedDefinition(key);
            if (cachedTaskDefinition != null)
                return cachedTaskDefinition;

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(TaskQueryBuilder.GetTaskQuery(_tableSchema), connection))
                {
                    command.Parameters.Add(new SqlParameter("@ApplicationName", SqlDbType.VarChar, 200)).Value = applicationName;
                    command.Parameters.Add(new SqlParameter("@TaskName", SqlDbType.VarChar, 200)).Value = taskName;
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var task = new TaskDefinition();
                        task.TaskSecondaryId = int.Parse(reader["TaskSecondaryId"].ToString());

                        CacheTaskDefinition(key, task);
                        return task;
                    }
                }
            }

            return null;
        }

        private TaskDefinition GetCachedDefinition(string taskKey)
        {
            lock (_myCacheSyncObj)
            {
                if (_cachedTaskDefinitions.ContainsKey(taskKey))
                {
                    var definition = _cachedTaskDefinitions[taskKey];
                    if ((definition.CachedAt - DateTime.UtcNow).TotalSeconds < 300)
                        return definition.TaskDefinition;
                }
            }

            return null;
        }

        private void CacheTaskDefinition(string taskKey, TaskDefinition taskDefinition)
        {
            lock (_myCacheSyncObj)
            {
                if (_cachedTaskDefinitions.ContainsKey(taskKey))
                {
                    _cachedTaskDefinitions[taskKey] = new CachedTaskDefinition()
                    {
                        TaskDefinition = taskDefinition,
                        CachedAt = DateTime.UtcNow
                    };
                }
                else
                {
                    _cachedTaskDefinitions.Add(taskKey, new CachedTaskDefinition()
                    {
                        TaskDefinition = taskDefinition,
                        CachedAt = DateTime.UtcNow
                    });
                }
            }
        }

        private TaskDefinition InsertNewTask(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(TaskQueryBuilder.InsertTaskQuery(_tableSchema), connection))
                {
                    command.Parameters.Add(new SqlParameter("@ApplicationName", SqlDbType.VarChar, 200)).Value = applicationName;
                    command.Parameters.Add(new SqlParameter("@TaskName", SqlDbType.VarChar, 200)).Value = taskName;

                    var task = new TaskDefinition();
                    task.TaskSecondaryId = (int)command.ExecuteScalar();

                    string key = applicationName + "::" + taskName;
                    CacheTaskDefinition(key, task);
                    return task;
                }
            }
        }
    }
}
