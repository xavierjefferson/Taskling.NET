using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Transactions;
using Taskling.Events;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.Tasks;
using Taskling.SqlServer.Tokens.Executions;
using Taskling.Tasks;

namespace Taskling.SqlServer.Tests.Helpers;

public class ExecutionsHelper
{
    public ExecutionsHelper()
    {
        TaskRepository.ClearCache();
        ConnectionStore.Instance.SetConnection(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            new ClientConnectionSettings(TestConstants.TestConnectionString, TestConstants.QueryTimeout));
    }


    public void DeleteRecordsOfApplication(string applicationName)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            using (var ts = new TransactionScope())
            {
                var command = connection.CreateCommand();
                command.CommandText = DeleteExecutionTokenQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.ExecuteNonQuery();
                ts.Complete();
            }
        }
    }

    public void SetKeepAlive(int taskExecutionId)
    {
        SetKeepAlive(taskExecutionId, DateTime.UtcNow);
    }

    public void SetKeepAlive(int taskExecutionId, DateTime keepAliveDateTime)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = SetKeepAliveQuery;
            command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
            command.Parameters.Add("@KeepAliveDateTime", SqlDbType.DateTime).Value = keepAliveDateTime;

            command.ExecuteNonQuery();
        }
    }

    public DateTime GetLastKeepAlive(int taskDefinitionId)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = GetLastKeepAliveQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            return (DateTime)command.ExecuteScalar();
        }
    }

    public Tuple<EventType, string> GetLastEvent(int taskDefinitionId)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = GetLastEventQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            var reader = command.ExecuteReader();
            if (reader.Read())
            {
                var result = new Tuple<EventType, string>(
                    (EventType)reader.GetInt32(0),
                    reader.GetString(1));

                reader.Close();

                return result;
            }

            return null;
        }
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
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = InsertTaskQuery;
            command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
            command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
            var reader = command.ExecuteReader();
            while (reader.Read()) return reader.GetInt32("TaskDefinitionId");
        }

        return -1;
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

    private const string DeleteExecutionTokenQuery = @"
DELETE TEV FROM [Taskling].[TaskExecutionEvent] TEV
JOIN [Taskling].[TaskExecution] TE ON TEV.TaskExecutionId = TE.TaskExecutionId
JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName;

DELETE bE FROM [Taskling].BlockExecution be inner join [Taskling].[TaskExecution] TE on te.taskexecutionid= be.taskexecutionid
JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName;

DELETE TE FROM [Taskling].[TaskExecution] TE
JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName;

dELETE q FROM [taskling].forceblockqueue q inner join [taskling].[block] b on b.blockid = q.blockid inner join [Taskling].[TaskDefinition] t on b.taskdefinitionid = t.taskdefinitionid
WHERE t.ApplicationName = @ApplicationName;

dELETE q FROM [taskling].listblockitem q inner join [taskling].[block] b on b.blockid = q.blockid inner join [Taskling].[TaskDefinition] t on b.taskdefinitionid = t.taskdefinitionid
WHERE t.ApplicationName = @ApplicationName;

DELETE b FROM [taskling].[block] b inner join [Taskling].[TaskDefinition] t on b.taskdefinitionid = t.taskdefinitionid
WHERE t.ApplicationName = @ApplicationName;

DELETE FROM [Taskling].[TaskDefinition] 
WHERE ApplicationName = @ApplicationName;

DELETE FROM [Taskling].[ForceBlockQueue];";

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
        InsertExecutionToken(taskDefinitionId, new List<Tuple<ExecutionTokenStatus, int>>
        {
            new(ExecutionTokenStatus.Unlimited, 0)
        });
    }

    public void InsertUnavailableExecutionToken(int taskDefinitionId)
    {
        InsertExecutionToken(taskDefinitionId, new List<Tuple<ExecutionTokenStatus, int>>
        {
            new(ExecutionTokenStatus.Unavailable, 0)
        });
    }

    public void InsertAvailableExecutionToken(int taskDefinitionId, int count = 1)
    {
        var list = new List<Tuple<ExecutionTokenStatus, int>>();
        for (var i = 0; i < count; i++)
            list.Add(new Tuple<ExecutionTokenStatus, int>(ExecutionTokenStatus.Available, 0));

        InsertExecutionToken(taskDefinitionId, list);
    }

    public void InsertExecutionToken(int taskDefinitionId, List<Tuple<ExecutionTokenStatus, int>> tokens)
    {
        var tokenString = GenerateTokensString(tokens);

        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = InsertExecutionTokenQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            command.Parameters.Add("@ExecutionTokens", SqlDbType.VarChar, 8000).Value = tokenString;
            command.ExecuteNonQuery();
        }
    }

    private string GenerateTokensString(List<Tuple<ExecutionTokenStatus, int>> tokens)
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
            sb.Append(((int)token.Item1).ToString());
            sb.Append(",G:");
            sb.Append(token.Item2);

            counter++;
        }

        return sb.ToString();
    }

    public ExecutionTokenList GetExecutionTokens(string applicationName, string taskName)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = GetExecutionTokensQuery;
            command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
            command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
            var result = command.ExecuteScalar().ToString();

            return ExecutionTokenRepository.ParseTokensString(result);
        }
    }

    public ExecutionTokenStatus GetExecutionTokenStatus(string applicationName, string taskName)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = GetExecutionTokensQuery;
            command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
            command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
            var result = command.ExecuteScalar().ToString();
            if (string.IsNullOrEmpty(result))
                return ExecutionTokenStatus.Available;

            return (ExecutionTokenStatus)int.Parse(result.Substring(result.IndexOf("S:") + 2, 1));
        }
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
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = InsertKeepAliveTaskExecutionQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            command.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.VarChar, 200)).Value =
                Environment.MachineName;
            command.Parameters.Add(new SqlParameter("@TaskDeathMode", SqlDbType.Int)).Value =
                (int)TaskDeathMode.KeepAlive;
            command.Parameters.Add(new SqlParameter("@KeepAliveInterval", SqlDbType.Time)).Value = keepAliveInterval;
            command.Parameters.Add(new SqlParameter("@KeepAliveDeathThreshold", SqlDbType.Time)).Value =
                keepAliveDeathThreshold;
            command.Parameters.Add(new SqlParameter("@StartedAt", SqlDbType.DateTime)).Value = startedAt;
            command.Parameters.Add(new SqlParameter("@FailedTaskRetryLimit", SqlDbType.Int)).Value = 3;
            command.Parameters.Add(new SqlParameter("@DeadTaskRetryLimit", SqlDbType.Int)).Value = 3;

            if (completedAt.HasValue)
                command.Parameters.Add(new SqlParameter("@CompletedAt", SqlDbType.DateTime)).Value = completedAt;
            else
                command.Parameters.Add(new SqlParameter("@CompletedAt", SqlDbType.DateTime)).Value = DBNull.Value;

            return (int)command.ExecuteScalar();
        }
    }

    public int InsertOverrideTaskExecution(int taskDefinitionId, TimeSpan overrideThreshold)
    {
        return InsertOverrideTaskExecution(taskDefinitionId, overrideThreshold, DateTime.UtcNow, DateTime.UtcNow);
    }

    public int InsertOverrideTaskExecution(int taskDefinitionId, TimeSpan overrideThreshold, DateTime startedAt,
        DateTime? completedAt)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = InsertOverrideTaskExecutionQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            command.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.VarChar, 200)).Value =
                Environment.MachineName;
            command.Parameters.Add(new SqlParameter("@TaskDeathMode", SqlDbType.Int)).Value =
                (int)TaskDeathMode.Override;
            command.Parameters.Add(new SqlParameter("@OverrideThreshold", SqlDbType.Time)).Value = overrideThreshold;
            command.Parameters.Add(new SqlParameter("@StartedAt", SqlDbType.DateTime)).Value = startedAt;
            command.Parameters.Add(new SqlParameter("@FailedTaskRetryLimit", SqlDbType.Int)).Value = 3;
            command.Parameters.Add(new SqlParameter("@DeadTaskRetryLimit", SqlDbType.Int)).Value = 3;

            if (completedAt.HasValue)
                command.Parameters.Add(new SqlParameter("@CompletedAt", SqlDbType.DateTime)).Value = completedAt;
            else
                command.Parameters.Add(new SqlParameter("@CompletedAt", SqlDbType.DateTime)).Value = DBNull.Value;

            return (int)command.ExecuteScalar();
        }
    }

    public void SetTaskExecutionAsCompleted(int taskExecutionId)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = UpdateTaskExecutionStatusQuery;
            command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;

            command.ExecuteNonQuery();
        }
    }

    public void SetLastExecutionAsDead(int taskDefinitionId)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = SetLastExecutionAsDeadQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            command.ExecuteNonQuery();
        }
    }

    public bool GetBlockedStatusOfLastExecution(int taskDefinitionId)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = GetLastTaskExecutionQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            var reader = command.ExecuteReader();
            if (reader.Read())
            {
                var result = (bool)reader["Blocked"];

                reader.Close();

                return result;
            }

            return false;
        }
    }

    public string GetLastExecutionVersion(int taskDefinitionId)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = GetLastTaskExecutionQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            var reader = command.ExecuteReader();
            if (reader.Read())
            {
                var result = reader["TasklingVersion"].ToString();

                reader.Close();

                return result;
            }

            return string.Empty;
        }
    }

    public string GetLastExecutionHeader(int taskDefinitionId)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = GetLastTaskExecutionQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            var reader = command.ExecuteReader();
            if (reader.Read())
            {
                var result = reader["ExecutionHeader"].ToString();

                reader.Close();

                return result;
            }

            return string.Empty;
        }
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
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = InsertCriticalSectionTokenQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
            command.Parameters.Add("@Status", SqlDbType.Int).Value = status;
            command.ExecuteNonQuery();
        }
    }

    public int GetQueueCount(int taskExecutionId)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = GetQueueCountQuery;
            command.Parameters.Add("@TaskExecutionId", SqlDbType.VarChar).Value = taskExecutionId;
            return (int)command.ExecuteScalar();
        }
    }

    public void InsertIntoCriticalSectionQueue(int taskDefinitionId, int queueIndex, int taskExecutionId)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = InsertIntoCriticalSectionQueueQuery;
            command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            command.Parameters.Add("@CsQueue", SqlDbType.VarChar).Value = queueIndex + "," + taskExecutionId;
            command.ExecuteNonQuery();
        }
    }

    public int GetCriticalSectionTokenStatus(string applicationName, string taskName)
    {
        using (var connection = new SqlConnection(TestConstants.TestConnectionString))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = GetCriticalSectionTokenStatusByTaskExecutionQuery;
            command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
            command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
            return (int)command.ExecuteScalar();
        }
    }

    #endregion .: Critical Sections :.
}