using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.TaskExecution.QueryBuilders;

namespace Taskling.SqlServer.Tasks
{
    public class TaskRepository : DbOperationsService, ITaskRepository
    {
        private static object _myCacheSyncObj = new object();
        private static object _getTaskObj = new object();
        private static Dictionary<string, CachedTaskDefinition> _cachedTaskDefinitions = new Dictionary<string, CachedTaskDefinition>();
                
        public TaskDefinition EnsureTaskDefinition(TaskId taskId)
        {
            lock (_getTaskObj)
            {
                var taskDefinition = GetTask(taskId);
                if (taskDefinition != null)
                {
                    return taskDefinition;
                }
                else
                {
                    // wait a random amount of time in case two threads or two instances of this repository 
                    // independently belive that the task doesn't exist
                    Thread.Sleep(new Random(Guid.NewGuid().GetHashCode()).Next(2000));
                    taskDefinition = GetTask(taskId);
                    if (taskDefinition != null)
                    {
                        return taskDefinition;
                    }

                    return InsertNewTask(taskId);
                }
            }
        }

        public DateTime GetLastTaskCleanUpTime(TaskId taskId)
        {
            using (var connection = CreateNewConnection(taskId))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.GetLastCleanUpTimeQuery, connection))
                {
                    command.Parameters.Add(new SqlParameter("@ApplicationName", SqlDbType.VarChar, 200)).Value = taskId.ApplicationName;
                    command.Parameters.Add(new SqlParameter("@TaskName", SqlDbType.VarChar, 200)).Value = taskId.TaskName;
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            if (reader["LastCleaned"] == DBNull.Value)
                                return DateTime.MinValue;

                            return DateTime.Parse(reader["LastCleaned"].ToString());
                        }
                    }
                }
            }

            return DateTime.MinValue;
        }

        public void SetLastCleaned(TaskId taskId)
        {
            using (var connection = CreateNewConnection(taskId))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.SetLastCleanUpTimeQuery, connection))
                {
                    command.Parameters.Add(new SqlParameter("@ApplicationName", SqlDbType.VarChar, 200)).Value = taskId.ApplicationName;
                    command.Parameters.Add(new SqlParameter("@TaskName", SqlDbType.VarChar, 200)).Value = taskId.TaskName;
                    command.ExecuteNonQuery();
                }
            }
        }

        public static void ClearCache()
        {
            lock (_myCacheSyncObj)
            {
                _cachedTaskDefinitions.Clear();
            }
        }

        private TaskDefinition GetTask(TaskId taskId)
        {
            return GetCachedDefinition(taskId);
        }

        private TaskDefinition GetCachedDefinition(TaskId taskId)
        {
            lock (_myCacheSyncObj)
            {
                string key = taskId.ApplicationName + "::" + taskId.TaskName;

                if (_cachedTaskDefinitions.ContainsKey(key))
                {
                    var definition = _cachedTaskDefinitions[key];
                    if ((definition.CachedAt - DateTime.UtcNow).TotalSeconds < 300)
                        return definition.TaskDefinition;
                }
                else
                {
                    var task = LoadTask(taskId);
                    CacheTaskDefinition(key, task);
                    return task;
                }
            }

            return null;
        }

        private TaskDefinition LoadTask(TaskId taskId)
        {
            using (var connection = CreateNewConnection(taskId))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.GetTaskQuery, connection))
                {
                    command.Parameters.Add(new SqlParameter("@ApplicationName", SqlDbType.VarChar, 200)).Value = taskId.ApplicationName;
                    command.Parameters.Add(new SqlParameter("@TaskName", SqlDbType.VarChar, 200)).Value = taskId.TaskName;
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var task = new TaskDefinition();
                            task.TaskDefinitionId = int.Parse(reader["TaskDefinitionId"].ToString());

                            return task;
                        }
                    }
                }
            }

            return null;
        }

        private void CacheTaskDefinition(string taskKey, TaskDefinition taskDefinition)
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

        private TaskDefinition InsertNewTask(TaskId taskId)
        {
            using (var connection = CreateNewConnection(taskId))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.InsertTaskQuery, connection))
                {
                    command.Parameters.Add(new SqlParameter("@ApplicationName", SqlDbType.VarChar, 200)).Value = taskId.ApplicationName;
                    command.Parameters.Add(new SqlParameter("@TaskName", SqlDbType.VarChar, 200)).Value = taskId.TaskName;

                    var task = new TaskDefinition();
                    task.TaskDefinitionId = (int)command.ExecuteScalar();

                    string key = taskId.ApplicationName + "::" + taskId.TaskName;

                    lock (_myCacheSyncObj)
                    {
                        CacheTaskDefinition(key, task);
                    }
                    return task;
                }
            }
        }
    }
}
