using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.TaskExecution.QueryBuilders;

namespace Taskling.SqlServer.Tasks
{
    public class TaskRepository : DbOperationsService, ITaskRepository
    {
        private static SemaphoreSlim _cacheSemaphore = new SemaphoreSlim(1, 1);
        private static SemaphoreSlim _getTaskSemaphore = new SemaphoreSlim(1, 1);
        private static Dictionary<string, CachedTaskDefinition> _cachedTaskDefinitions = new Dictionary<string, CachedTaskDefinition>();
                
        public async Task<TaskDefinition> EnsureTaskDefinitionAsync(TaskId taskId)
        {
            await _getTaskSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                var taskDefinition = await GetTaskAsync(taskId).ConfigureAwait(false);
                if (taskDefinition != null)
                {
                    return taskDefinition;
                }
                else
                {
                    // wait a random amount of time in case two threads or two instances of this repository 
                    // independently belive that the task doesn't exist
                    await Task.Delay(new Random(Guid.NewGuid().GetHashCode()).Next(2000)).ConfigureAwait(false);
                    taskDefinition = await GetTaskAsync(taskId).ConfigureAwait(false);
                    if (taskDefinition != null)
                    {
                        return taskDefinition;
                    }

                    return await InsertNewTaskAsync(taskId).ConfigureAwait(false);
                }
            }
            finally
            {
                _getTaskSemaphore.Release();
            }
        }

        public async Task<DateTime> GetLastTaskCleanUpTimeAsync(TaskId taskId)
        {
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.GetLastCleanUpTimeQuery, connection))
                {
                    command.Parameters.Add(new SqlParameter("@ApplicationName", SqlDbType.VarChar, 200)).Value = taskId.ApplicationName;
                    command.Parameters.Add(new SqlParameter("@TaskName", SqlDbType.VarChar, 200)).Value = taskId.TaskName;
                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync().ConfigureAwait(false))
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

        public async Task SetLastCleanedAsync(TaskId taskId)
        {
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.SetLastCleanUpTimeQuery, connection))
                {
                    command.Parameters.Add(new SqlParameter("@ApplicationName", SqlDbType.VarChar, 200)).Value = taskId.ApplicationName;
                    command.Parameters.Add(new SqlParameter("@TaskName", SqlDbType.VarChar, 200)).Value = taskId.TaskName;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }

        public static void ClearCache()
        {
            _cacheSemaphore.Wait();
            try
            {
                _cachedTaskDefinitions.Clear();
            }
            finally
            {
                _cacheSemaphore.Release();
            }
        }

        private async Task<TaskDefinition> GetTaskAsync(TaskId taskId)
        {
            return await GetCachedDefinitionAsync(taskId).ConfigureAwait(false);
        }

        private async Task<TaskDefinition> GetCachedDefinitionAsync(TaskId taskId)
        {
            await _cacheSemaphore.WaitAsync().ConfigureAwait(false);
            try
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
                    var task = await LoadTaskAsync(taskId).ConfigureAwait(false);
                    CacheTaskDefinition(key, task);
                    return task;
                }
            }
            finally
            {
                _cacheSemaphore.Release();
            }

            return null;
        }

        private async Task<TaskDefinition> LoadTaskAsync(TaskId taskId)
        {
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.GetTaskQuery, connection))
                {
                    command.Parameters.Add(new SqlParameter("@ApplicationName", SqlDbType.VarChar, 200)).Value = taskId.ApplicationName;
                    command.Parameters.Add(new SqlParameter("@TaskName", SqlDbType.VarChar, 200)).Value = taskId.TaskName;
                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync().ConfigureAwait(false))
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

        private async Task<TaskDefinition> InsertNewTaskAsync(TaskId taskId)
        {
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                using (var command = new SqlCommand(TaskQueryBuilder.InsertTaskQuery, connection))
                {
                    command.Parameters.Add(new SqlParameter("@ApplicationName", SqlDbType.VarChar, 200)).Value = taskId.ApplicationName;
                    command.Parameters.Add(new SqlParameter("@TaskName", SqlDbType.VarChar, 200)).Value = taskId.TaskName;

                    var task = new TaskDefinition();
                    task.TaskDefinitionId = (int)await command.ExecuteScalarAsync().ConfigureAwait(false);

                    string key = taskId.ApplicationName + "::" + taskId.TaskName;

                    await _cacheSemaphore.WaitAsync().ConfigureAwait(false);
                    try
                    {
                        CacheTaskDefinition(key, task);
                    }
                    finally
                    {
                        _cacheSemaphore.Release();
                    }

                    return task;
                }
            }
        }
    }
}
