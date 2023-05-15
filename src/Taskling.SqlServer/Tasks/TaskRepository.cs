﻿using System.Transactions;
using Microsoft.EntityFrameworkCore;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Models;
using TransactionScopeRetryHelper;
using TaskDefinition = Taskling.InfrastructureContracts.TaskExecution.TaskDefinition;

namespace Taskling.SqlServer.Tasks;

public class TaskRepository : DbOperationsService, ITaskRepository
{
    private static readonly SemaphoreSlim CacheSemaphore = new(1, 1);
    private static readonly SemaphoreSlim GetTaskSemaphore = new(1, 1);
    private static readonly Dictionary<string, CachedTaskDefinition> CachedTaskDefinitions = new();

    public TaskRepository(IConnectionStore connectionStore, IDbContextFactoryEx dbContextFactoryEx) : base(
        connectionStore, dbContextFactoryEx)
    {
    }

    public async Task<TaskDefinition> EnsureTaskDefinitionAsync(TaskId taskId)
    {

        return await GetTaskSemaphore.WrapAsync(async () =>
        {
            var taskDefinition = await GetTaskAsync(taskId).ConfigureAwait(false);
            if (taskDefinition != null)
            {
                return taskDefinition;
            }
            else
            {
                async Task<TaskDefinition> insertFunc()
                {
                    taskDefinition = await GetTaskAsync(taskId).ConfigureAwait(false);
                    if (taskDefinition != null) return taskDefinition;

                    return await InsertNewTaskAsync(taskId).ConfigureAwait(false);
                }

                // wait a random amount of time in case two threads or two instances of this repository 
                // independently belive that the task doesn't exist
                await Task.Delay(new Random(Guid.NewGuid().GetHashCode()).Next(2000)).ConfigureAwait(false);

                if (System.Transactions.Transaction.Current == null)
                {
                    using (var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                    {
                        try
                        {
                            return await insertFunc();
                        }
                        finally
                        {
                            transactionScope.Complete();
                        }
                    }
                }
                else
                {
                    return await insertFunc();
                }

            }



        }).ConfigureAwait(false);
    }

    public async Task<DateTime> GetLastTaskCleanUpTimeAsync(TaskId taskId)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(taskId))
            {
                var lastCleaned = await GetTaskDefinitionsByTaskIdAsync(taskId, dbContext)
                    .Select(i => i.LastCleaned).FirstOrDefaultAsync().ConfigureAwait(false);
                return lastCleaned ?? DateTime.MinValue;
            }
        });
    }

    public async Task SetLastCleanedAsync(TaskId taskId)
    {
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(taskId))
            {
                var taskDefinition = await GetTaskDefinitionsByTaskIdAsync(taskId, dbContext)
                    .FirstOrDefaultAsync()
                    .ConfigureAwait(false);
                if (taskDefinition != null)
                {
                    taskDefinition.LastCleaned = DateTime.UtcNow;
                    dbContext.TaskDefinitions.Update(taskDefinition);
                    await dbContext.SaveChangesAsync();
                }
            }
        });
    }

    public void ClearCache()
    {
        CacheSemaphore.Wrap(() => { CachedTaskDefinitions.Clear(); });
    }

    private static IQueryable<Models.TaskDefinition> GetTaskDefinitionsByTaskIdAsync(TaskId taskId,
        TasklingDbContext dbContext)
    {
        return dbContext.TaskDefinitions
            .Where(i => i.TaskName == taskId.TaskName && i.ApplicationName == taskId.ApplicationName);
    }

    private async Task<TaskDefinition?> GetTaskAsync(TaskId taskId)
    {
        return await GetCachedDefinitionAsync(taskId).ConfigureAwait(false);
    }

    private async Task<TaskDefinition?> GetCachedDefinitionAsync(TaskId taskId)
    {
        return await CacheSemaphore.WrapAsync(async () =>
        {
            var key = taskId.GetUniqueKey();

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

            return null;
        });
    }

    private async Task<TaskDefinition?> LoadTaskAsync(TaskId taskId)
    {
        using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))
        {
            return await GetTaskDefinitionsByTaskIdAsync(taskId, dbContext)
                .Select(i => new TaskDefinition { TaskDefinitionId = i.TaskDefinitionId }).FirstOrDefaultAsync()
                .ConfigureAwait(false);
        }
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


            var task = new TaskDefinition
            {
                TaskDefinitionId = taskDefinition.TaskDefinitionId
            };

            var key = taskId.GetUniqueKey();

            CacheSemaphore.Wrap(() => { CacheTaskDefinition(key, task); });


            return task;
        }
    }
}