using System.Transactions;
using Microsoft.EntityFrameworkCore;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Blocks;

namespace Taskling.SqlServer.Tasks;

public class TaskRepository : DbOperationsService, ITaskRepository
{
    private static readonly SemaphoreSlim CacheSemaphore = new(1, 1);
    private static readonly SemaphoreSlim GetTaskSemaphore = new(1, 1);
    private static readonly Dictionary<string, CachedTaskDefinition> CachedTaskDefinitions = new();

    public async Task<TaskDefinition> EnsureTaskDefinitionAsync(TaskId taskId)
    {
        await GetTaskSemaphore.WaitAsync().ConfigureAwait(false);

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
                using (var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                {
                    try
                    {
                        taskDefinition = await GetTaskAsync(taskId).ConfigureAwait(false);
                        if (taskDefinition != null) return taskDefinition;

                        return await InsertNewTaskAsync(taskId).ConfigureAwait(false);
                    }
                    finally
                    {
                        transactionScope.Complete();
                    }
                }
            }
        }
        finally
        {
            GetTaskSemaphore.Release();
        }
    }

    public async Task<DateTime> GetLastTaskCleanUpTimeAsync(TaskId taskId)
    {
        return await RetryHelper.WithRetry(async (transactionScope) =>
        {
            using (var dbContext = await GetDbContextAsync(taskId))
            {
                var tuple = await dbContext.TaskDefinitions
                    .Where(i => i.TaskName == taskId.TaskName && i.ApplicationName == taskId.ApplicationName)
                    .Select(i => new { i.LastCleaned }).FirstOrDefaultAsync().ConfigureAwait(false);
                if (tuple == null) return DateTime.MinValue;
                return tuple.LastCleaned ?? DateTime.MinValue;
            }

            return DateTime.MinValue;
        });

    }

    public async Task SetLastCleanedAsync(TaskId taskId)
    {
        await RetryHelper.WithRetry(async (transactionScope) =>
       {
           using (var dbContext = await GetDbContextAsync(taskId))
           {
               var taskDefinitions = await dbContext.TaskDefinitions
                    .Where(i => i.TaskName == taskId.TaskName && i.ApplicationName == taskId.ApplicationName).ToListAsync()
                    .ConfigureAwait(false);
               foreach (var taskDefinition in taskDefinitions)
               {
                   taskDefinition.LastCleaned = DateTime.UtcNow;
                   dbContext.TaskDefinitions.Update(taskDefinition);
               }

               await dbContext.SaveChangesAsync();
           }
       });

    }

    public static void ClearCache()
    {
        CacheSemaphore.Wait();
        try
        {
            CachedTaskDefinitions.Clear();
        }
        finally
        {
            CacheSemaphore.Release();
        }
    }

    private async Task<TaskDefinition?> GetTaskAsync(TaskId taskId)
    {
        return await GetCachedDefinitionAsync(taskId).ConfigureAwait(false);
    }

    private async Task<TaskDefinition> GetCachedDefinitionAsync(TaskId taskId)
    {
        await CacheSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            var key = taskId.ApplicationName + "::" + taskId.TaskName;

            if (CachedTaskDefinitions.ContainsKey(key))
            {
                var definition = CachedTaskDefinitions[key];
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
            CacheSemaphore.Release();
        }

        return null;
    }

    private async Task<TaskDefinition?> LoadTaskAsync(TaskId taskId)
    {
        using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))
        {
            var tuple = await dbContext.TaskDefinitions
                .Where(i => i.ApplicationName == taskId.ApplicationName && i.TaskName == taskId.TaskName)
                .Select(i => new { i.TaskDefinitionId }).FirstOrDefaultAsync().ConfigureAwait(false);
            if (tuple != null)
                return new TaskDefinition
                {
                    TaskDefinitionId = tuple.TaskDefinitionId
                };
        }

        return null;
    }

    private void CacheTaskDefinition(string taskKey, TaskDefinition taskDefinition)
    {
        if (CachedTaskDefinitions.ContainsKey(taskKey))
            CachedTaskDefinitions[taskKey] = new CachedTaskDefinition
            {
                TaskDefinition = taskDefinition,
                CachedAt = DateTime.UtcNow
            };
        else
            CachedTaskDefinitions.Add(taskKey, new CachedTaskDefinition
            {
                TaskDefinition = taskDefinition,
                CachedAt = DateTime.UtcNow
            });
    }

    private async Task<TaskDefinition> InsertNewTaskAsync(TaskId taskId)
    {
        using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))
        {
            var taskDefinition = new Models.TaskDefinition
            { ApplicationName = taskId.ApplicationName, TaskName = taskId.TaskName };
            await dbContext.TaskDefinitions.AddAsync(taskDefinition);
            await dbContext.SaveChangesAsync();


            var task = new TaskDefinition();
            task.TaskDefinitionId = taskDefinition.TaskDefinitionId;

            var key = taskId.ApplicationName + "::" + taskId.TaskName;

            await CacheSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                CacheTaskDefinition(key, task);
            }
            finally
            {
                CacheSemaphore.Release();
            }

            return task;
        }
    }
}