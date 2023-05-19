﻿using System.Reflection;
using System.Transactions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Taskling.Extensions;
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
    private readonly ILogger<TaskRepository> _logger;

    public TaskRepository(IConnectionStore connectionStore, IDbContextFactoryEx dbContextFactoryEx,
        ILogger<TaskRepository> logger, ILoggerFactory loggerFactory) : base(
        connectionStore, dbContextFactoryEx, loggerFactory.CreateLogger<DbOperationsService>())
    {
        _logger = logger;
    }

    public async Task<TaskDefinition> EnsureTaskDefinitionAsync(TaskId taskId)
    {
        _logger.Debug("feb002b1-9960-4bd9-a273-9d72a6d66785");
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return await GetTaskSemaphore.WrapAsync(async () =>
        {
            var taskDefinition = await GetTaskAsync(taskId).ConfigureAwait(false);
            if (taskDefinition != null) return taskDefinition;

            async Task<TaskDefinition> insertFunc()
            {
                taskDefinition = await GetTaskAsync(taskId).ConfigureAwait(false);
                if (taskDefinition != null) return taskDefinition;

                return await InsertNewTaskAsync(taskId).ConfigureAwait(false);
            }

            // wait a random amount of time in case two threads or two instances of this repository 
            // independently belive that the task doesn't exist
            await Task.Delay(new Random(Guid.NewGuid().GetHashCode()).Next(2000)).ConfigureAwait(false);

            if (Transaction.Current == null)
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

            return await insertFunc();
        }).ConfigureAwait(false);
    }

    public async Task<DateTime> GetLastTaskCleanUpTimeAsync(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
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
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
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

    private IQueryable<Models.TaskDefinition> GetTaskDefinitionsByTaskIdAsync(TaskId taskId,
        TasklingDbContext dbContext)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return dbContext.TaskDefinitions
            .Where(i => i.TaskName == taskId.TaskName && i.ApplicationName == taskId.ApplicationName);
    }

    private async Task<TaskDefinition?> GetTaskAsync(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return await GetCachedDefinitionAsync(taskId).ConfigureAwait(false);
    }

    private async Task<TaskDefinition?> GetCachedDefinitionAsync(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return await CacheSemaphore.WrapAsync(async () =>
        {
            var key = taskId.GetUniqueKey();

            if (CachedTaskDefinitions.ContainsKey(key))
            {
                var taskDefinition = CachedTaskDefinitions[key];
                if ((taskDefinition.CachedAt - DateTime.UtcNow).TotalSeconds < 300)
                    return taskDefinition.TaskDefinition;
            }
            else
            {
                var taskDefinition = await LoadTaskAsync(taskId).ConfigureAwait(false);
                CacheTaskDefinition(key, taskDefinition);
                return taskDefinition;
            }

            return null;
        });
    }

    private async Task<TaskDefinition?> LoadTaskAsync(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("10d1f644-51ea-45c9-b12f-ce2aa9f60d33");
        using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))
        {
            return await GetTaskDefinitionsByTaskIdAsync(taskId, dbContext)
                .Select(i => new TaskDefinition
                {
                    TaskDefinitionId = i.TaskDefinitionId,
                    ApplicationName = i.ApplicationName,
                    TaskName = i.TaskName
                }).FirstOrDefaultAsync()
                .ConfigureAwait(false);
        }
    }

    private void CacheTaskDefinition(string taskKey, TaskDefinition taskDefinition)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("9858af09-3cab-44b5-8de6-8cbf7b6f787f");
        if (CachedTaskDefinitions.ContainsKey(taskKey))
        {
            _logger.Debug("911f827a-1493-481a-8352-6de8b0f3d83d");
            CachedTaskDefinitions[taskKey] = new CachedTaskDefinition
            {
                TaskDefinition = taskDefinition,
                CachedAt = DateTime.UtcNow
            };
        }
        else
        {
            _logger.Debug("b339464d-8f93-4616-885c-2a612fd96206");
            CachedTaskDefinitions.Add(taskKey, new CachedTaskDefinition
            {
                TaskDefinition = taskDefinition,
                CachedAt = DateTime.UtcNow
            });
        }
    }

    private async Task<TaskDefinition> InsertNewTaskAsync(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("440ad14c-d923-4d37-b3bd-cd47f55d2bdc");
        using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))
        {
            var modelsTaskDefinition = new Models.TaskDefinition
                { ApplicationName = taskId.ApplicationName, TaskName = taskId.TaskName };
            await dbContext.TaskDefinitions.AddAsync(modelsTaskDefinition);
            await dbContext.SaveChangesAsync();


            var taskDefinition = new TaskDefinition
            {
                TaskDefinitionId = modelsTaskDefinition.TaskDefinitionId,
                ApplicationName = taskId.ApplicationName,
                TaskName = taskId.TaskName
            };

            var key = taskId.GetUniqueKey();

            CacheSemaphore.Wrap(() => { CacheTaskDefinition(key, taskDefinition); });


            return taskDefinition;
        }
    }
}