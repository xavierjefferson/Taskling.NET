using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Bogus;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Taskling.Events;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Tests.Repositories.Given_BlockRepository;
using Taskling.SqlServer.Tokens.CriticalSections;
using Taskling.SqlServer.Tokens.Executions;
using Taskling.Tasks;
using TransactionScopeRetryHelper;
using TaskDefinition = Taskling.SqlServer.Models.TaskDefinition;

namespace Taskling.SqlServer.Tests.Helpers;

public class ExecutionsHelper : RepositoryBase, IExecutionsHelper
{
    private static bool _ranFirstExecution;
    private readonly IConnectionStore _connectionStore;
    private readonly Faker<TestTaskInfo> _faker;
    private readonly ILogger<ExecutionsHelper> _logger;


    public ExecutionsHelper(IConnectionStore connectionStore, ITaskRepository taskRepository,
        ILogger<ExecutionsHelper> logger)
    {
        _logger = logger;
        _connectionStore = connectionStore;

        taskRepository.ClearCache();
        _faker = new Faker<TestTaskInfo>().RuleFor(i => i.ApplicationName, i => i.Database.Column())
            .RuleFor(i => i.TaskName, i => Guid.NewGuid().ToString());
        var testTaskInfo = _faker.Generate();
        CurrentTaskId = new TaskId(testTaskInfo.ApplicationName, testTaskInfo.TaskName);
        if (_ranFirstExecution == false)
        {
            using (var dbContext = DbContextOptionsHelper.GetDbContext())
            {
                dbContext.Database.EnsureCreated();
                dbContext.TaskExecutionEvents.RemoveRange(dbContext.TaskExecutionEvents);
                dbContext.BlockExecutions.RemoveRange(dbContext.BlockExecutions);
                dbContext.TaskExecutions.RemoveRange(dbContext.TaskExecutions);
                dbContext.ForceBlockQueues.RemoveRange(dbContext.ForceBlockQueues);
                dbContext.ListBlockItems.RemoveRange(dbContext.ListBlockItems);
                dbContext.Blocks.RemoveRange(dbContext.Blocks);
                dbContext.TaskDefinitions.RemoveRange(dbContext.TaskDefinitions);
                dbContext.SaveChanges();
            }

            _ranFirstExecution = true;
        }

        _connectionStore.SetConnection(CurrentTaskId,
            new ClientConnectionSettings(TestConstants.GetTestConnectionString(), TestConstants.QueryTimeout));
    }

    public TaskId CurrentTaskId { get; }

    public void DeleteRecordsOfApplication(string applicationName)
    {
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
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
                dbContext.ForceBlockQueues.RemoveRange(dbContext.ForceBlockQueues.Include(i => i.Block)
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
            using (var dbContext = GetDbContext())
            {
                try
                {
                    var z = dbContext.TaskExecutions.Attach(new Models.TaskExecution
                    { TaskExecutionId = taskExecutionId, LastKeepAlive = keepAliveDateTime });
                    z.Property(i => i.LastKeepAlive).IsModified = true;
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
            using (var dbContext = GetDbContext())
            {
                var z = dbContext.TaskExecutions.Where(i => i.TaskDefinitionId == taskDefinitionId)
                    .Select(i => new { i.LastKeepAlive }).FirstOrDefault();
                return z == null ? DateTime.MinValue : z.LastKeepAlive;
            }
        });
    }

    /// <summary>
    ///     private const string GetLastEventQuery = @"SELECT [EventType]
    ///     ,[Message]
    ///     FROM[Taskling].[TaskExecutionEvent]
    ///     TEE
    ///     JOIN Taskling.TaskExecution AS TE ON TEE.TaskExecutionId = TE.TaskExecutionId
    ///     WHERE TE.TaskDefinitionId = @TaskDefinitionId
    ///     ORDER BY 1 DESC";
    /// </summary>
    /// <param name="taskDefinitionId"></param>
    /// <returns></returns>
    public GetLastEventResponse GetLastEvent(long taskDefinitionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var z = dbContext.TaskExecutionEvents.OrderByDescending(i => i.TaskExecutionEventId)
                    .Where(i => i.TaskExecution.TaskDefinitionId == taskDefinitionId)
                    .Select(i => new { i.EventType, i.Message }).FirstOrDefault();

                if (z == null) return null;
                return new GetLastEventResponse((EventType)z.EventType, z.Message);
            }
        });
    }


    #region .: Tasks :.

    public long InsertTask(TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
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

    #endregion .: Tasks :.


    #region .: Queries :.

    #region .: Execution Tokens :.

    private const string InsertExecutionTokenQuery = @"
        UPDATE [Taskling].[TaskDefinition]
        SET [ExecutionTokens] = @ExecutionTokens
        WHERE [TaskDefinitionId] = @TaskDefinitionId;";

    private const string GetExecutionTokensQuery = @"SELECT ExecutionTokens
FROM [Taskling].[TaskDefinition]
WHERE ApplicationName = @ApplicationName
AND TaskName = @TaskName";

    #endregion .: Execution Tokens :.

    #region .: Delete All :.

    #endregion .: Delete All :.

    #region .: Tasks :.

    private const string InsertTaskQuery =
        @"INSERT INTO [Taskling].[TaskDefinition]([ApplicationName],[TaskName],[UserCsStatus],[ClientCsStatus])
VALUES(@ApplicationName,@TaskName, 1, 1);

SELECT [ApplicationName]
      ,[TaskName]
      ,[TaskDefinitionId]
  FROM [Taskling].[TaskDefinition]
WHERE ApplicationName = @ApplicationName
AND TaskName = @TaskName";

    #endregion .: Tasks :.

    #region .: Keep Alive :.

    private const string SetKeepAliveQuery = @"
UPDATE TE
SET [LastKeepAlive] = @KeepAliveDateTime
FROM [Taskling].[TaskExecution] TE
WHERE [TaskExecutionId] = @TaskExecutionId;
";

    private const string GetLastKeepAliveQuery = @"SELECT MAX(LastKeepAlive)
FROM [Taskling].[TaskExecution] 
WHERE [TaskDefinitionId] = @TaskDefinitionId";

    #endregion .: Keep Alive :.

    #region .: Task Executions :.

    private const string InsertKeepAliveTaskExecutionQuery = @"INSERT INTO [Taskling].[TaskExecution]
           ([TaskDefinitionId]
           ,[StartedAt]
           ,[LastKeepAlive]
           ,[ServerName]
           ,[TaskDeathMode]
           ,[KeepAliveInterval]
           ,[KeepAliveDeathThreshold]
           ,[FailedTaskRetryLimit]
           ,[DeadTaskRetryLimit]
           ,[Failed]
           ,[Blocked]
           ,[TasklingVersion])
     VALUES
           (@TaskDefinitionId
           ,@StartedAt
           ,@CompletedAt
           ,@ServerName
           ,@TaskDeathMode
           ,@KeepAliveInterval
           ,@KeepAliveDeathThreshold
           ,@FailedTaskRetryLimit
           ,@DeadTaskRetryLimit
           ,0
           ,0
           ,'N/A');

SELECT CAST(SCOPE_IDENTITY() AS INT);
";

    private const string InsertOverrideTaskExecutionQuery = @"INSERT INTO [Taskling].[TaskExecution]
           ([TaskDefinitionId]
           ,[StartedAt]
           ,[LastKeepAlive]
           ,[ServerName]
           ,[TaskDeathMode]
           ,[OverrideThreshold]
           ,[FailedTaskRetryLimit]
           ,[DeadTaskRetryLimit]
           ,[Failed]
           ,[Blocked]
           ,[TasklingVersion])
     VALUES
           (@TaskDefinitionId
           ,@StartedAt
           ,@CompletedAt
           ,@ServerName
           ,@TaskDeathMode
           ,@OverrideThreshold
           ,@FailedTaskRetryLimit
           ,@DeadTaskRetryLimit
           ,0
           ,0
           ,'N/A');

SELECT CAST(SCOPE_IDENTITY() AS INT);
";

    private const string UpdateTaskExecutionStatusQuery = @"
UPDATE [TasklingDb].[Taskling].[TaskExecution]
SET [CompletedAt] = GETUTCDATE()
WHERE TaskExecutionId = @TaskExecutionId
";

    private const string SetLastExecutionAsDeadQuery = @"UPDATE [Taskling].[TaskExecution]
           SET CompletedAt = null,
            LastKeepAlive = DATEADD(HOUR, -12, GETUTCDATE()),
            StartedAt = DATEADD(HOUR, -12, GETUTCDATE())
WHERE TaskDefinitionId = @TaskDefinitionId
AND TaskExecutionId = (SELECT MAX(TaskExecutionId) FROM [Taskling].[TaskExecution])";


    private const string GetLastTaskExecutionQuery = @"SELECT *
FROM [Taskling].[TaskExecution] TE
WHERE TE.TaskDefinitionId = @TaskDefinitionId
ORDER BY 1 DESC";

    #endregion .: Task Executions :.

    #region .: Critical Sections :.

    private const string InsertCriticalSectionTokenQuery = @"UPDATE [Taskling].[TaskDefinition]
SET [UserCsStatus] = @Status
,[UserCsTaskExecutionId] = @TaskExecutionId
,[HoldLockTaskExecutionId] = @TaskExecutionId
WHERE TaskDefinitionId = @TaskDefinitionId";

    private const string GetQueueCountQuery = @"SELECT COUNT(*)
FROM [Taskling].[TaskDefinition]
WHERE [UserCsQueue] LIKE '%' + @TaskExecutionId + '%'";

    private const string InsertIntoCriticalSectionQueueQuery = @"UPDATE [Taskling].[TaskDefinition]
SET [UserCsQueue] = COALESCE([UserCsQueue],'') + '|' + @CsQueue
WHERE TaskDefinitionId = @TaskDefinitionId";

    private const string GetCriticalSectionTokenStatusByTaskExecutionQuery = @"SELECT T.[UserCsStatus]
FROM [Taskling].[TaskExecution] TE
JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName";

    #endregion .: Critical Sections :.

    #endregion .: Queries :.


    #region .: Execution Tokens :.

    public void InsertUnlimitedExecutionToken(long taskDefinitionId)
    {
        InsertExecutionToken(taskDefinitionId, new ExecInfoList
        {
            new(ExecutionTokenStatus.Unlimited, 0)
        });
    }

    public void InsertUnavailableExecutionToken(long taskDefinitionId)
    {
        InsertExecutionToken(taskDefinitionId, new ExecInfoList
        {
            new(ExecutionTokenStatus.Unavailable, 0)
        });
    }

    public void InsertAvailableExecutionToken(long taskDefinitionId, int count = 1)
    {
        var list = new ExecInfoList();
        for (var i = 0; i < count; i++)
            list.Add(new Execinfo(ExecutionTokenStatus.Available, 0));

        InsertExecutionToken(taskDefinitionId, list);
    }

    public void InsertExecutionToken(long taskDefinitionId, ExecInfoList tokens)
    {
        RetryHelper.WithRetry(() =>
        {
            var tokenString = GenerateTokensString(tokens);
            using (var dbContext = GetDbContext())
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


    private string GenerateTokensString(ExecInfoList tokens)
    {
        return JsonConvert.SerializeObject(tokens);
    }

    public ExecutionTokenList GetExecutionTokens(TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
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
            using (var dbContext = GetDbContext())
            {
                var result = dbContext.TaskDefinitions
                    .Where(i => i.ApplicationName == taskId.ApplicationName && i.TaskName == taskId.TaskName)
                    .Select(i => i.ExecutionTokens).FirstOrDefault();
                if (string.IsNullOrEmpty(result))
                    return ExecutionTokenStatus.Available;
                return JsonConvert.DeserializeObject<ExecInfoList>(result)[0].Status;
            }
        });
    }

    #endregion .: Execution Tokens :.


    #region .: Task Executions :.

    public long InsertKeepAliveTaskExecution(long taskDefinitionId)
    {
        return InsertKeepAliveTaskExecution(taskDefinitionId, new TimeSpan(0, 0, 20), new TimeSpan(0, 1, 0));
    }

    public long InsertOverrideTaskExecution(long taskDefinitionId)
    {
        return InsertOverrideTaskExecution(taskDefinitionId, new TimeSpan(0, 1, 0));
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
            using (var dbContext = GetDbContext())
            {
                var taskExecution = new Models.TaskExecution
                {
                    TaskDefinitionId = taskDefinitionId,
                    StartedAt = startedAt,
                    LastKeepAlive = completedAt ?? DateTime.MinValue,
                    ServerName = Environment.MachineName,

                    TaskDeathMode = (int)TaskDeathMode.KeepAlive,
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
            using (var dbContext = GetDbContext())
            {
                var taskExecution = new Models.TaskExecution
                {
                    TaskDefinitionId = taskDefinitionId,
                    ServerName = Environment.MachineName,
                    LastKeepAlive = completedAt ?? DateTime.MinValue,
                    TaskDeathMode = (int)TaskDeathMode.Override,
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
            using (var dbContext = GetDbContext())
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
            using (var dbContext = GetDbContext())
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
            using (var dbContext = GetDbContext())
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
            using (var dbContext = GetDbContext())
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
            using (var dbContext = GetDbContext())
            {
                return dbContext.TaskExecutions.Include(i => i.TaskDefinition)
                    .Where(i => i.TaskDefinitionId == taskDefinitionId).Select(i => i.ExecutionHeader).FirstOrDefault();
            }
        });
    }

    #endregion .: Task Executions :.


    #region .: Critical Sections :.

    public void InsertUnavailableCriticalSectionToken(long taskDefinitionId, long taskExecutionId)
    {
        InsertCriticalSectionToken(taskDefinitionId, taskExecutionId, 0);
    }

    public void InsertAvailableCriticalSectionToken(long taskDefinitionId, long taskExecutionId)
    {
        InsertCriticalSectionToken(taskDefinitionId, taskExecutionId, 1);
    }

    private void InsertCriticalSectionToken(long taskDefinitionId, long taskExecutionId, int status)
    {
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
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

    public int GetQueueCount(long taskExecutionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var userCsQueuesAsString = dbContext.TaskDefinitions.Select(i => i.UserCsQueue).ToList();
                var countMatching = userCsQueuesAsString.Count(i =>
                    CsQueueSerializer.Deserialize(i).Any(i => i.TaskExecutionId == taskExecutionId));
                return countMatching;
            }
        });
    }

    public void InsertIntoCriticalSectionQueue(long taskDefinitionId, int queueIndex, long taskExecutionId)
    {
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var taskDefinitions = dbContext.TaskDefinitions.Where(i => i.TaskDefinitionId == taskDefinitionId)
                    .ToList();

                foreach (var taskDefinition in taskDefinitions)
                {
                    var list = CsQueueSerializer.Deserialize(taskDefinition.UserCsQueue);
                    list.Add(new CriticalSectionQueueItem { TaskExecutionId = taskExecutionId });
                    taskDefinition.UserCsQueue = CsQueueSerializer.Serialize(list);
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
            using (var dbContext = GetDbContext())
            {
                var a = dbContext.TaskExecutions.Include(i => i.TaskDefinition)
                    .Where(i => i.TaskDefinition.ApplicationName == taskId.ApplicationName &&
                                i.TaskDefinition.TaskName == taskId.TaskName)
                    .Select(i => i.TaskDefinition.UserCsStatus).FirstOrDefault();
                return a;
            }
        });
    }

    #endregion .: Critical Sections :.
}

public class GetLastEventResponse
{
    public GetLastEventResponse(EventType eventType, string message)
    {
        EventType = eventType;
        Message = message;
    }

    public EventType EventType { get; }
    public string Message { get; }
}

public class Execinfo
{
    public Execinfo(ExecutionTokenStatus status, int grantedTaskExecutionId)
    {
        Status = status;
        GrantedTaskExecutionId = grantedTaskExecutionId;
    }

    public Execinfo()
    {
    }

    [JsonProperty("I")] public Guid TokenId { get; set; } = Guid.NewGuid();

    [JsonProperty("S")] public ExecutionTokenStatus Status { get; set; }

    [JsonProperty("G")] public int GrantedTaskExecutionId { get; set; }
}

public class ExecInfoList : List<Execinfo>
{
}