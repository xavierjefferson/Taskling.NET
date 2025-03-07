﻿using System;
using System.Linq;
using Bogus;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Taskling.Configuration;
using Taskling.EntityFrameworkCore.AncilliaryServices;
using Taskling.EntityFrameworkCore.Tests.Repositories.Given_BlockRepository;
using Taskling.EntityFrameworkCore.Tokens.CriticalSections;
using Taskling.EntityFrameworkCore.Tokens.Executions;
using Taskling.Enums;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using TaskDefinition = Taskling.EntityFrameworkCore.Models.TaskDefinition;

namespace Taskling.EntityFrameworkCore.Tests.Helpers;

public class ExecutionsHelper : RepositoryBase, IExecutionsHelper
{
    private static bool _ranFirstExecution;
    private readonly IDbContextFactoryEx _dbContextFactory;
    private readonly Faker<TestTaskInfo> _faker;
    private readonly ILogger<ExecutionsHelper> _logger;

    public ExecutionsHelper(ITaskRepository taskRepository,
        ILogger<ExecutionsHelper> logger, IDbContextFactoryEx dbContextFactory,
        ITaskConfigurationReader taskConfigurationReader)
    {
        _logger = logger;
        _dbContextFactory = dbContextFactory;
        taskRepository.ClearCache();
        _faker = new Faker<TestTaskInfo>().RuleFor(i => i.ApplicationName, i => i.Database.Column())
            .RuleFor(i => i.TaskName, i => Guid.NewGuid().ToString());
        var testTaskInfo = _faker.Generate();
        CurrentTaskId = new TaskId(testTaskInfo.ApplicationName, testTaskInfo.TaskName);
        var testTaskConfigurationReader = taskConfigurationReader as TestTaskConfigurationReader;


        if (_ranFirstExecution == false)
        {
            var dummy = new TaskId(Guid.NewGuid().ToString(), Guid.NewGuid().ToString());

            testTaskConfigurationReader.Add(dummy,
                new ConfigurationOptions
                {
                    ConnectionString = Startup.GetConnectionString(),
                    CommandTimeoutSeconds = 120, ExpiresInSeconds = 0
                });

            using (var dbContext = _dbContextFactory.GetDbContext(dummy))
            {
                dbContext.Database.EnsureCreated();
                dbContext.TaskExecutionEvents.RemoveRange(dbContext.TaskExecutionEvents);
                dbContext.BlockExecutions.RemoveRange(dbContext.BlockExecutions);
                dbContext.TaskExecutions.RemoveRange(dbContext.TaskExecutions);
                dbContext.ForcedBlockQueues.RemoveRange(dbContext.ForcedBlockQueues);
                dbContext.ListBlockItems.RemoveRange(dbContext.ListBlockItems);
                dbContext.Blocks.RemoveRange(dbContext.Blocks);
                dbContext.TaskDefinitions.RemoveRange(dbContext.TaskDefinitions);
                dbContext.SaveChanges();
            }

            _ranFirstExecution = true;
        }
    }

    public TaskId CurrentTaskId { get; }

    public void DeleteRecordsOfApplication(string applicationName)
    {
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                dbContext.TaskExecutionEvents
                    .RemoveRange(dbContext.TaskExecutionEvents.Include(i => i.TaskExecution)
                        .ThenInclude(i => i.TaskDefinition).Where(i =>
                            i.TaskExecution.TaskDefinition.ApplicationName == applicationName));
                dbContext.BlockExecutions.RemoveRange(dbContext.BlockExecutions.Include(i => i.TaskExecution)
                    .ThenInclude(i => i.TaskDefinition).Include(i => i.Block).ThenInclude(i => i.TaskDefinition)
                    .Where(i => i.TaskExecution.TaskDefinition.ApplicationName == applicationName ||
                                i.Block.TaskDefinition.ApplicationName == applicationName));
                dbContext.TaskExecutions.RemoveRange(dbContext.TaskExecutions.Include(i => i.TaskDefinition)
                    .Where(i => i.TaskDefinition.ApplicationName == applicationName));
                dbContext.ForcedBlockQueues.RemoveRange(dbContext.ForcedBlockQueues.Include(i => i.Block)
                    .ThenInclude(i => i.TaskDefinition)
                    .Where(i => i.Block.TaskDefinition.ApplicationName == applicationName));
                dbContext.ListBlockItems.RemoveRange(dbContext.ListBlockItems.Include(i => i.Block)
                    .ThenInclude(i => i.TaskDefinition)
                    .Where(i => i.Block.TaskDefinition.ApplicationName == applicationName));
                dbContext.Blocks.RemoveRange(dbContext.Blocks.Include(i => i.TaskDefinition)
                    .Where(i => i.TaskDefinition.ApplicationName == applicationName));
                dbContext.TaskDefinitions.RemoveRange(
                    dbContext.TaskDefinitions.Where(i => i.ApplicationName == applicationName));
                dbContext.SaveChanges();
            }
        });
    }

    public void SetKeepAlive(long taskExecutionId)
    {
        SetKeepAlive(taskExecutionId, DateTime.UtcNow);
    }

    public void SetKeepAlive(long taskExecutionId, DateTime keepAliveDateTime)
    {
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                try
                {
                    var entityEntry = dbContext.TaskExecutions.Attach(new Models.TaskExecution
                        { TaskExecutionId = taskExecutionId, LastKeepAlive = keepAliveDateTime });
                    entityEntry.Property(i => i.LastKeepAlive).IsModified = true;
                    dbContext.SaveChanges();
                }
                catch (DbUpdateException)
                {
                }
            }
        });
    }

    public DateTime GetLastKeepAlive(long taskDefinitionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                var tmp = dbContext.TaskExecutions.Where(i => i.TaskDefinitionId == taskDefinitionId)
                    .Select(i => new { i.LastKeepAlive }).FirstOrDefault();
                return tmp?.LastKeepAlive ?? DateTime.MinValue;
            }
        });
    }

    /// <summary>
    /// </summary>
    /// <param name="taskDefinitionId"></param>
    /// <returns></returns>
    public GetLastEventResponse GetLastEvent(long taskDefinitionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                var tmp = dbContext.TaskExecutionEvents.OrderByDescending(i => i.TaskExecutionEventId)
                    .Where(i => i.TaskExecution.TaskDefinitionId == taskDefinitionId)
                    .Select(i => new { i.EventType, i.Message }).FirstOrDefault();

                if (tmp == null) return null;
                return new GetLastEventResponse((EventTypeEnum)tmp.EventType, tmp.Message);
            }
        });
    }

    public long InsertTask(TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                var taskDefinition = new TaskDefinition
                {
                    ApplicationName = taskId.ApplicationName,
                    TaskName = taskId.TaskName,
                    UserCsStatus = 1,
                    ClientCsStatus = 1
                };
                dbContext.TaskDefinitions.Add(taskDefinition);
                dbContext.SaveChanges();
                return taskDefinition.TaskDefinitionId;
            }
        });
    }

    public long InsertTask(string applicationName, string taskName)
    {
        return InsertTask(new TaskId(applicationName, taskName));
    }

    public void InsertUnlimitedExecutionToken(long taskDefinitionId)
    {
        InsertExecutionToken(taskDefinitionId, new ExecutionTokenList
        {
            new(ExecutionTokenStatus.Unlimited, 0)
        });
    }

    public void InsertUnavailableExecutionToken(long taskDefinitionId)
    {
        InsertExecutionToken(taskDefinitionId, new ExecutionTokenList
        {
            new(ExecutionTokenStatus.Unavailable, 0)
        });
    }

    public void InsertAvailableExecutionToken(long taskDefinitionId, int count = 1)
    {
        var list = new ExecutionTokenList();
        for (var i = 0; i < count; i++)
            list.Add(new ExecutionToken(ExecutionTokenStatus.Available, 0));

        InsertExecutionToken(taskDefinitionId, list);
    }

    public void InsertExecutionToken(long taskDefinitionId, ExecutionTokenList tokens)
    {
        RetryHelper.WithRetry(() =>
        {
            var tokenString = GenerateTokensString(tokens);
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                try
                {
                    var entity = dbContext.TaskDefinitions.Attach(new TaskDefinition
                        { TaskDefinitionId = taskDefinitionId });
                    entity.Entity.ExecutionTokens = tokenString;
                    entity.Property(i => i.ExecutionTokens).IsModified = true;
                    dbContext.SaveChanges();
                    entity.State = EntityState.Detached;
                }
                catch (DbUpdateException)
                {
                    //do nothing
                }
            }
        });
    }

    public ExecutionTokenList GetExecutionTokens(TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                var tmp = dbContext.TaskDefinitions
                    .Where(i => i.ApplicationName == taskId.ApplicationName && i.TaskName == taskId.TaskName)
                    .Select(i => i.ExecutionTokens).FirstOrDefault();
                return ExecutionTokenList.Deserialize(tmp);
            }
        });
    }

    public ExecutionTokenStatus GetExecutionTokenStatus(TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                var result = dbContext.TaskDefinitions
                    .Where(i => i.ApplicationName == taskId.ApplicationName && i.TaskName == taskId.TaskName)
                    .Select(i => i.ExecutionTokens).FirstOrDefault();
                if (string.IsNullOrWhiteSpace(result))
                    return ExecutionTokenStatus.Available;
                return ExecutionTokenList.Deserialize(result)[0].Status;
            }
        });
    }

    public long InsertKeepAliveTaskExecution(long taskDefinitionId)
    {
        return InsertKeepAliveTaskExecution(taskDefinitionId, TimeSpans.TwentySeconds, TimeSpans.OneMinute);
    }

    public long InsertOverrideTaskExecution(long taskDefinitionId)
    {
        return InsertOverrideTaskExecution(taskDefinitionId, TimeSpans.OneMinute);
    }

    public long InsertKeepAliveTaskExecution(long taskDefinitionId, TimeSpan keepAliveInterval,
        TimeSpan keepAliveDeathThreshold)
    {
        return InsertKeepAliveTaskExecution(taskDefinitionId, keepAliveInterval, keepAliveDeathThreshold,
            DateTime.UtcNow, DateTime.UtcNow);
    }

    public long InsertKeepAliveTaskExecution(long taskDefinitionId, TimeSpan keepAliveInterval,
        TimeSpan keepAliveDeathThreshold, DateTime startedAt, DateTime? completedAt)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                var taskExecution = new Models.TaskExecution
                {
                    TaskDefinitionId = taskDefinitionId,
                    StartedAt = startedAt,
                    LastKeepAlive = completedAt ?? DateTime.MinValue,
                    ServerName = Environment.MachineName,

                    TaskDeathMode = (int)TaskDeathModeEnum.KeepAlive,
                    KeepAliveInterval = keepAliveInterval,
                    KeepAliveDeathThreshold = keepAliveDeathThreshold,
                    FailedTaskRetryLimit = 3,
                    DeadTaskRetryLimit = 3,
                    Failed = false,
                    Blocked = false,
                    TasklingVersion = "N/A"
                };
                dbContext.TaskExecutions.Add(taskExecution);
                dbContext.SaveChanges();
                return taskExecution.TaskExecutionId;
            }
        });
    }

    public long InsertOverrideTaskExecution(long taskDefinitionId, TimeSpan overrideThreshold)
    {
        return InsertOverrideTaskExecution(taskDefinitionId, overrideThreshold, DateTime.UtcNow, DateTime.UtcNow);
    }

    public long InsertOverrideTaskExecution(long taskDefinitionId, TimeSpan overrideThreshold, DateTime startedAt,
        DateTime? completedAt)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                var taskExecution = new Models.TaskExecution
                {
                    TaskDefinitionId = taskDefinitionId,
                    ServerName = Environment.MachineName,
                    LastKeepAlive = completedAt ?? DateTime.MinValue,
                    TaskDeathMode = (int)TaskDeathModeEnum.Override,
                    OverrideThreshold = overrideThreshold,
                    StartedAt = startedAt,
                    FailedTaskRetryLimit = 3,
                    DeadTaskRetryLimit = 3,

                    Failed = false,
                    Blocked = false,
                    TasklingVersion = "N/A"
                };
                dbContext.TaskExecutions.Add(taskExecution);
                dbContext.SaveChanges();
                return taskExecution.TaskExecutionId;
            }
        });
    }

    public void SetTaskExecutionAsCompleted(long taskExecutionId)
    {
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                try
                {
                    var z = dbContext.TaskExecutions.Attach(
                        new Models.TaskExecution { TaskExecutionId = taskExecutionId });
                    z.Entity.CompletedAt = DateTime.UtcNow;
                    z.Property(i => i.CompletedAt).IsModified = true;
                    dbContext.SaveChanges();
                }
                catch (DbUpdateException)
                {
                }
            }
        });
    }

    public void SetLastExecutionAsDead(long taskDefinitionId)
    {
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                var a = dbContext.TaskExecutions.GroupBy(i => 1).Select(i => i.Max(j => j.TaskExecutionId));
                var taskExecution = a
                    .Join(dbContext.TaskExecutions, i => i, j => j.TaskExecutionId, (i, j) => j)
                    .FirstOrDefault(j => j.TaskDefinitionId == taskDefinitionId);
                if (taskExecution != null)
                {
                    taskExecution.CompletedAt = null;
                    taskExecution.StartedAt = taskExecution.LastKeepAlive = DateTime.UtcNow.AddHours(-12);
                    dbContext.TaskExecutions.Update(taskExecution);
                }

                dbContext.SaveChanges();
            }
        });
    }

    public bool GetBlockedStatusOfLastExecution(long taskDefinitionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                return dbContext.TaskExecutions.OrderByDescending(i => i.TaskExecutionId)
                    .Where(i => i.TaskDefinitionId == taskDefinitionId).Select(i => i.Blocked).FirstOrDefault();
            }
        });
    }

    public string GetLastExecutionVersion(long taskDefinitionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                return dbContext.TaskExecutions.OrderByDescending(i => i.TaskExecutionId)
                    .Where(i => i.TaskDefinitionId == taskDefinitionId).Select(i => i.TasklingVersion).FirstOrDefault();
            }
        });
    }

    public string GetLastExecutionHeader(long taskDefinitionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                return dbContext.TaskExecutions.Include(i => i.TaskDefinition)
                    .Where(i => i.TaskDefinitionId == taskDefinitionId).Select(i => i.ExecutionHeader).FirstOrDefault();
            }
        });
    }

    public void InsertUnavailableCriticalSectionToken(long taskDefinitionId, long taskExecutionId)
    {
        InsertCriticalSectionToken(taskDefinitionId, taskExecutionId, 0);
    }

    public void InsertAvailableCriticalSectionToken(long taskDefinitionId, long taskExecutionId)
    {
        InsertCriticalSectionToken(taskDefinitionId, taskExecutionId, 1);
    }

    public int GetQueueCount(long taskExecutionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                var userCsQueuesAsString = dbContext.TaskDefinitions.Select(i => i.UserCsQueue).ToList();
                var countMatching = userCsQueuesAsString.Count(i =>
                    CriticalSectionQueueSerializer.Deserialize(i).Any(i => i.TaskExecutionId == taskExecutionId));
                return countMatching;
            }
        });
    }

    public void InsertIntoCriticalSectionQueue(long taskDefinitionId, int queueIndex, long taskExecutionId)
    {
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                var taskDefinitions = dbContext.TaskDefinitions.Where(i => i.TaskDefinitionId == taskDefinitionId)
                    .ToList();

                foreach (var taskDefinition in taskDefinitions)
                {
                    var list = CriticalSectionQueueSerializer.Deserialize(taskDefinition.UserCsQueue);
                    list.Add(new CriticalSectionQueueItem { TaskExecutionId = taskExecutionId });
                    taskDefinition.UserCsQueue = CriticalSectionQueueSerializer.Serialize(list);
                    dbContext.TaskDefinitions.Update(taskDefinition);
                }

                dbContext.SaveChanges();
            }
        });
    }

    public int GetCriticalSectionTokenStatus(TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                var a = dbContext.TaskExecutions.Include(i => i.TaskDefinition)
                    .Where(i => i.TaskDefinition.ApplicationName == taskId.ApplicationName &&
                                i.TaskDefinition.TaskName == taskId.TaskName)
                    .Select(i => i.TaskDefinition.UserCsStatus).FirstOrDefault();
                return a;
            }
        });
    }

    private string GenerateTokensString(ExecutionTokenList tokens)
    {
        return tokens.Serialize();
    }

    private void InsertCriticalSectionToken(long taskDefinitionId, long taskExecutionId, int status)
    {
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(CurrentTaskId))
            {
                try
                {
                    var entityEntry = dbContext.TaskDefinitions.Attach(new TaskDefinition
                    {
                        TaskDefinitionId = taskDefinitionId,
                        UserCsStatus = status,
                        HoldLockTaskExecutionId = taskExecutionId,
                        UserCsTaskExecutionId = taskExecutionId
                    });
                    entityEntry.Property(i => i.UserCsStatus).IsModified = true;
                    entityEntry.Property(i => i.UserCsTaskExecutionId).IsModified = true;
                    entityEntry.Property(i => i.HoldLockTaskExecutionId).IsModified = true;
                    dbContext.SaveChanges();
                }
                catch (DbUpdateException)
                {
                }
            }
        });
    }
}