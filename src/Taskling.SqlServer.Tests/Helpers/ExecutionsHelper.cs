﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Taskling.Events;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.Tasks;
using Taskling.SqlServer.Tokens.Executions;
using Taskling.Tasks;
using TaskDefinition = Taskling.SqlServer.Models.TaskDefinition;

namespace Taskling.SqlServer.Tests.Helpers;

public class ExecutionsHelper : RepositoryBase, IExecutionsHelper
{
    public ExecutionsHelper(IConnectionStore connectionStore, ITaskRepository taskRepository)
    {
        taskRepository.ClearCache();
        connectionStore.SetConnection(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            new ClientConnectionSettings(TestConstants.TestConnectionString, TestConstants.QueryTimeout));
    }


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

            //            using (var connection = GetConnection())
            //            {
            //                using (var ts = new TransactionScope())
            //                {
            //                    var command = connection.CreateCommand();
            //                    command.CommandText = @"
            //DELETE TEV FROM [Taskling].[TaskExecutionEvent] TEV
            //JOIN [Taskling].[TaskExecution] TE ON TEV.TaskExecutionId = TE.TaskExecutionId
            //JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId
            //WHERE T.ApplicationName = @ApplicationName;

            //DELETE bE FROM [Taskling].BlockExecution be inner join [Taskling].[TaskExecution] TE on te.taskexecutionid= be.taskexecutionid
            //JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId
            //WHERE T.ApplicationName = @ApplicationName;

            //DELETE TE FROM [Taskling].[TaskExecution] TE
            //JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId
            //WHERE T.ApplicationName = @ApplicationName;

            //dELETE q FROM [taskling].forceblockqueue q inner join [taskling].[block] b on b.blockid = q.blockid inner join [Taskling].[TaskDefinition] t on b.taskdefinitionid = t.taskdefinitionid
            //WHERE t.ApplicationName = @ApplicationName;

            //dELETE q FROM [taskling].listblockitem q inner join [taskling].[block] b on b.blockid = q.blockid inner join [Taskling].[TaskDefinition] t on b.taskdefinitionid = t.taskdefinitionid
            //WHERE t.ApplicationName = @ApplicationName;

            //DELETE b FROM [taskling].[block] b inner join [Taskling].[TaskDefinition] t on b.taskdefinitionid = t.taskdefinitionid
            //WHERE t.ApplicationName = @ApplicationName;

            //DELETE FROM [Taskling].[TaskDefinition] 
            //WHERE ApplicationName = @ApplicationName;

            //DELETE FROM [Taskling].[ForceBlockQueue];";
            //                    command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
            //                    command.ExecuteNonQuery();
            //                    ts.Complete();
            //                }
            //            }
        });
    }

    public void SetKeepAlive(int taskExecutionId)
    {
        SetKeepAlive(taskExecutionId, DateTime.UtcNow);
    }

    public void SetKeepAlive(int taskExecutionId, DateTime keepAliveDateTime)
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
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = SetKeepAliveQuery;
            //    command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
            //    command.Parameters.Add("@KeepAliveDateTime", SqlDbType.DateTime).Value = keepAliveDateTime;

            //    command.ExecuteNonQuery();
            //}
        });
    }

    public DateTime GetLastKeepAlive(int taskDefinitionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var z = dbContext.TaskExecutions.Where(i => i.TaskDefinitionId == taskDefinitionId)
                    .Select(i => new { i.LastKeepAlive }).FirstOrDefault();
                return z == null ? DateTime.MinValue : z.LastKeepAlive;
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetLastKeepAliveQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    return (DateTime)command.ExecuteScalar();
            //}
        });
    }

    public GetLastEventResponse GetLastEvent(int taskDefinitionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var z = dbContext.TaskExecutionEvents.OrderByDescending(i => i.TaskExecutionId)
                    .Where(i => i.TaskExecution.TaskDefinitionId == taskDefinitionId)
                    .Select(i => new { i.EventType, i.Message }).FirstOrDefault();

                if (z == null) return null;
                return new GetLastEventResponse((EventType)z.EventType, z.Message);
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetLastEventQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    var reader = command.ExecuteReader();
            //    if (reader.Read())
            //    {
            //        var result = new zuple(
            //            (EventType)reader.GetInt32(0),
            //            reader.GetString(1));

            //        reader.Close();

            //        return result;
            //    }

            //    return null;
            //}
        });
    }


    #region .: Tasks :.

    //public int CreateTaskAndExecutionToken(string applicationName, string taskName, int tokenCount = 1)
    //{
    //    var taskDefinitionId = InsertTask(applicationName, taskName);
    //    InsertUnavailableExecutionToken(taskDefinitionId, 0);

    //    return taskDefinitionId;
    //}

    public int InsertTask(string applicationName, string taskName)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var task = new TaskDefinition
                {
                    ApplicationName = applicationName,
                    TaskName = taskName,
                    UserCsStatus = 1,
                    ClientCsStatus = 1
                };
                dbContext.TaskDefinitions.Add(task);
                dbContext.SaveChanges();
                return task.TaskDefinitionId;
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertTaskQuery;
            //    command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
            //    command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
            //    var reader = command.ExecuteReader();
            //    while (reader.Read()) return reader.GetInt32("TaskDefinitionId");
            //}

            //return -1;
        });
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

    private const string GetLastEventQuery = @"SELECT [EventType]
      ,[Message]
FROM [Taskling].[TaskExecutionEvent] TEE
JOIN Taskling.TaskExecution AS TE ON TEE.TaskExecutionId = TE.TaskExecutionId
WHERE TE.TaskDefinitionId = @TaskDefinitionId
ORDER BY 1 DESC";

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

    public void InsertUnlimitedExecutionToken(int taskDefinitionId)
    {
        InsertExecutionToken(taskDefinitionId, new List<Execinfo>
        {
            new(ExecutionTokenStatus.Unlimited, 0)
        });
    }

    public void InsertUnavailableExecutionToken(int taskDefinitionId)
    {
        InsertExecutionToken(taskDefinitionId, new List<Execinfo>
        {
            new(ExecutionTokenStatus.Unavailable, 0)
        });
    }

    public void InsertAvailableExecutionToken(int taskDefinitionId, int count = 1)
    {
        var list = new List<Execinfo>();
        for (var i = 0; i < count; i++)
            list.Add(new Execinfo(ExecutionTokenStatus.Available, 0));

        InsertExecutionToken(taskDefinitionId, list);
    }

    public void InsertExecutionToken(int taskDefinitionId, List<Execinfo> tokens)
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
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertExecutionTokenQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    command.Parameters.Add("@ExecutionTokens", SqlDbType.VarChar, 8000).Value = tokenString;
            //    command.ExecuteNonQuery();
            //}
        });
    }

    private string GenerateTokensString(List<Execinfo> tokens)
    {
        var sb = new StringBuilder();
        var counter = 0;
        foreach (var token in tokens)
        {
            if (counter > 0)
                sb.Append("|");

            sb.Append("I:");
            sb.Append(Guid.NewGuid());
            sb.Append(",S:");
            sb.Append(((int)token.Status).ToString());
            sb.Append(",G:");
            sb.Append(token.GrantedTaskExecutionId);

            counter++;
        }

        return sb.ToString();
    }

    public ExecutionTokenList GetExecutionTokens(string applicationName, string taskName)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var tmp = dbContext.TaskDefinitions
                    .Where(i => i.ApplicationName == applicationName && i.TaskName == taskName)
                    .Select(i => i.ExecutionTokens).FirstOrDefault();
                return ExecutionTokenRepository.ParseTokensString(tmp);
                //var objectBlock = new Block()
                //{
                //    TaskDefinitionId = taskDefinitionId,
                //    CreatedDate = createdDate,
                //    ToNumber = toNumber,
                //    FromNumber = fromNumber,
                //    //ObjectData = JsonGenericSerializer.Serialize(objectData),
                //    BlockType = (int)BlockType.NumericRange
                //};
                //dbContext.Blocks.Add(objectBlock);
                //dbContext.SaveChanges();
                //return objectBlock.BlockId;
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetExecutionTokensQuery;
            //    command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
            //    command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
            //    var result = command.ExecuteScalar().ToString();


            //}
        });
    }

    public ExecutionTokenStatus GetExecutionTokenStatus(string applicationName, string taskName)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var result = dbContext.TaskDefinitions
                    .Where(i => i.ApplicationName == applicationName && i.TaskName == taskName)
                    .Select(i => i.ExecutionTokens).FirstOrDefault();
                //=> i.TaskExecutionId)
                //.Where(i => i.TaskDefinitionId == taskDefinitionId).Select(i => i.Blocked).FirstOrDefault();
                if (string.IsNullOrEmpty(result))
                    return ExecutionTokenStatus.Available;

                return (ExecutionTokenStatus)int.Parse(result.Substring(result.IndexOf("S:") + 2, 1));
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetExecutionTokensQuery;
            //    command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
            //    command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
            //    var result = command.ExecuteScalar().ToString();
            //    if (string.IsNullOrEmpty(result))
            //        return ExecutionTokenStatus.Available;

            //    return (ExecutionTokenStatus)int.Parse(result.Substring(result.IndexOf("S:") + 2, 1));
            //}
        });
    }

    #endregion .: Execution Tokens :.


    #region .: Task Executions :.

    public int InsertKeepAliveTaskExecution(int taskDefinitionId)
    {
        return InsertKeepAliveTaskExecution(taskDefinitionId, new TimeSpan(0, 0, 20), new TimeSpan(0, 1, 0));
    }

    public int InsertOverrideTaskExecution(int taskDefinitionId)
    {
        return InsertOverrideTaskExecution(taskDefinitionId, new TimeSpan(0, 1, 0));
    }

    public int InsertKeepAliveTaskExecution(int taskDefinitionId, TimeSpan keepAliveInterval,
        TimeSpan keepAliveDeathThreshold)
    {
        return InsertKeepAliveTaskExecution(taskDefinitionId, keepAliveInterval, keepAliveDeathThreshold,
            DateTime.UtcNow, DateTime.UtcNow);
    }

    public int InsertKeepAliveTaskExecution(int taskDefinitionId, TimeSpan keepAliveInterval,
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
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertKeepAliveTaskExecutionQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    command.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.VarChar, 200)).Value =
            //        Environment.MachineName;
            //    command.Parameters.Add(new SqlParameter("@TaskDeathMode", SqlDbType.Int)).Value =
            //        (int)TaskDeathMode.KeepAlive;
            //    command.Parameters.Add(new SqlParameter("@KeepAliveInterval", SqlDbType.Time)).Value =
            //        keepAliveInterval;
            //    command.Parameters.Add(new SqlParameter("@KeepAliveDeathThreshold", SqlDbType.Time)).Value =
            //        keepAliveDeathThreshold;
            //    command.Parameters.Add(new SqlParameter("@StartedAt", SqlDbType.DateTime)).Value = startedAt;
            //    command.Parameters.Add(new SqlParameter("@FailedTaskRetryLimit", SqlDbType.Int)).Value = 3;
            //    command.Parameters.Add(new SqlParameter("@DeadTaskRetryLimit", SqlDbType.Int)).Value = 3;

            //    if (completedAt.HasValue)
            //        command.Parameters.Add(new SqlParameter("@CompletedAt", SqlDbType.DateTime)).Value = completedAt;
            //    else
            //        command.Parameters.Add(new SqlParameter("@CompletedAt", SqlDbType.DateTime)).Value = DBNull.Value;

            //    return (int)command.ExecuteScalar();
            //}
        });
    }

    public int InsertOverrideTaskExecution(int taskDefinitionId, TimeSpan overrideThreshold)
    {
        return InsertOverrideTaskExecution(taskDefinitionId, overrideThreshold, DateTime.UtcNow, DateTime.UtcNow);
    }

    public int InsertOverrideTaskExecution(int taskDefinitionId, TimeSpan overrideThreshold, DateTime startedAt,
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
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertOverrideTaskExecutionQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    command.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.VarChar, 200)).Value =
            //        Environment.MachineName;
            //    command.Parameters.Add(new SqlParameter("@TaskDeathMode", SqlDbType.Int)).Value =
            //        (int)TaskDeathMode.Override;
            //    command.Parameters.Add(new SqlParameter("@OverrideThreshold", SqlDbType.Time)).Value =
            //        overrideThreshold;
            //    command.Parameters.Add(new SqlParameter("@StartedAt", SqlDbType.DateTime)).Value = startedAt;
            //    command.Parameters.Add(new SqlParameter("@FailedTaskRetryLimit", SqlDbType.Int)).Value = 3;
            //    command.Parameters.Add(new SqlParameter("@DeadTaskRetryLimit", SqlDbType.Int)).Value = 3;

            //    if (completedAt.HasValue)
            //        command.Parameters.Add(new SqlParameter("@CompletedAt", SqlDbType.DateTime)).Value = completedAt;
            //    else
            //        command.Parameters.Add(new SqlParameter("@CompletedAt", SqlDbType.DateTime)).Value = DBNull.Value;

            //    return (int)command.ExecuteScalar();
            //}
        });
    }

    public void SetTaskExecutionAsCompleted(int taskExecutionId)
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
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = UpdateTaskExecutionStatusQuery;
            //    command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;

            //    command.ExecuteNonQuery();
            //}
        });
    }

    public void SetLastExecutionAsDead(int taskDefinitionId)
    {
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var a = dbContext.TaskExecutions.GroupBy(i => 1).Select(i => i.Max(j => j.TaskExecutionId));
                var b = a
                    .Join(dbContext.TaskExecutions, i => i, j => j.TaskExecutionId, (i, j) => j)
                    .FirstOrDefault(j => j.TaskDefinitionId == taskDefinitionId);
                if (b != null)
                {
                    b.CompletedAt = null;
                    b.StartedAt = b.LastKeepAlive = DateTime.UtcNow.AddHours(-12);
                    dbContext.TaskExecutions.Update(b);
                }

                dbContext.SaveChanges();
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = SetLastExecutionAsDeadQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    command.ExecuteNonQuery();
            //}
        });
    }

    public bool GetBlockedStatusOfLastExecution(int taskDefinitionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.TaskExecutions.OrderByDescending(i => i.TaskExecutionId)
                    .Where(i => i.TaskDefinitionId == taskDefinitionId).Select(i => i.Blocked).FirstOrDefault();
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetLastTaskExecutionQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    var reader = command.ExecuteReader();
            //    if (reader.Read())
            //    {
            //        var result = (bool)reader["Blocked"];

            //        reader.Close();

            //        return result;
            //    }

            //    return false;
            //}
        });
    }

    public string GetLastExecutionVersion(int taskDefinitionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.TaskExecutions.OrderByDescending(i => i.TaskExecutionId)
                    .Where(i => i.TaskDefinitionId == taskDefinitionId).Select(i => i.TasklingVersion).FirstOrDefault();
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetLastTaskExecutionQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    var reader = command.ExecuteReader();
            //    if (reader.Read())
            //    {
            //        var result = reader["TasklingVersion"].ToString();

            //        reader.Close();

            //        return result;
            //    }

            //    return string.Empty;
            //}
        });
    }

    public string GetLastExecutionHeader(int taskDefinitionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.TaskExecutions.Include(i => i.TaskDefinition)
                    .Where(i => i.TaskDefinitionId == taskDefinitionId).Select(i => i.ExecutionHeader).FirstOrDefault();
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetLastTaskExecutionQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    var reader = command.ExecuteReader();
            //    if (reader.Read())
            //    {
            //        var result = reader["ExecutionHeader"].ToString();

            //        reader.Close();

            //        return result;
            //    }

            //    return string.Empty;
            //}
        });
    }

    #endregion .: Task Executions :.


    #region .: Critical Sections :.

    public void InsertUnavailableCriticalSectionToken(int taskDefinitionId, int taskExecutionId)
    {
        InsertCriticalSectionToken(taskDefinitionId, taskExecutionId, 0);
    }

    public void InsertAvailableCriticalSectionToken(int taskDefinitionId, int taskExecutionId)
    {
        InsertCriticalSectionToken(taskDefinitionId, taskExecutionId, 1);
    }

    private void InsertCriticalSectionToken(int taskDefinitionId, int taskExecutionId, int status)
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
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertCriticalSectionTokenQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
            //    command.Parameters.Add("@Status", SqlDbType.Int).Value = status;
            //    command.ExecuteNonQuery();
            //}
        });
    }

    public int GetQueueCount(int taskExecutionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.TaskDefinitions.Count(i => i.UserCsQueue.Contains(taskExecutionId.ToString()));
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetQueueCountQuery;
            //    command.Parameters.Add("@TaskExecutionId", SqlDbType.VarChar).Value = taskExecutionId;
            //    return (int)command.ExecuteScalar();
            //}
        });
    }

    public void InsertIntoCriticalSectionQueue(int taskDefinitionId, int queueIndex, int taskExecutionId)
    {
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var taskDefinitions = dbContext.TaskDefinitions.Where(i => i.TaskDefinitionId == taskDefinitionId)
                    .ToList();
                foreach (var taskDefinition in taskDefinitions)
                {
                    taskDefinition.UserCsQueue = $"{taskDefinition.UserCsQueue}|{queueIndex + "," + taskExecutionId}";
                    dbContext.TaskDefinitions.Update(taskDefinition);
                }

                dbContext.SaveChanges();
                //var entityEntry = dbContext.TaskDefinitions.Attach(new Models.TaskDefinition()
                //{
                //    TaskDefinitionId = taskDefinitionId,
                //    UserCsStatus = status,
                //    HoldLockTaskExecutionId = taskExecutionId,
                //    UserCsTaskExecutionId = taskExecutionId
                //});
                //entityEntry.Property(i => i.UserCsStatus).IsModified = true;
                //entityEntry.Property(i => i.UserCsTaskExecutionId).IsModified = true;
                //entityEntry.Property(i => i.HoldLockTaskExecutionId).IsModified = true;
                //dbContext.SaveChanges();
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertIntoCriticalSectionQueueQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    command.Parameters.Add("@CsQueue", SqlDbType.VarChar).Value = queueIndex + "," + taskExecutionId;
            //    command.ExecuteNonQuery();
            //}
        });
    }

    public int GetCriticalSectionTokenStatus(string applicationName, string taskName)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var a = dbContext.TaskExecutions.Include(i => i.TaskDefinition)
                    .Where(i => i.TaskDefinition.ApplicationName == applicationName &&
                                i.TaskDefinition.TaskName == taskName)
                    .Select(i => i.TaskDefinition.UserCsStatus).FirstOrDefault();
                return a;
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetCriticalSectionTokenStatusByTaskExecutionQuery;
            //    command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
            //    command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
            //    return (int)command.ExecuteScalar();
            //}
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

    public ExecutionTokenStatus Status { get; }
    public int GrantedTaskExecutionId { get; }
}