using System;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.Tokens.Executions;

namespace Taskling.SqlServer.Tests.Helpers;

public interface IExecutionsHelper
{
    TaskId CurrentTaskId { get; }
    void DeleteRecordsOfApplication(string applicationName);
    void SetKeepAlive(long taskExecutionId);
    void SetKeepAlive(long taskExecutionId, DateTime keepAliveDateTime);
    DateTime GetLastKeepAlive(long taskDefinitionId);
    GetLastEventResponse GetLastEvent(long taskDefinitionId);
    long InsertTask(string applicationName, string taskName);
    long InsertTask(TaskId taskId);
    void InsertUnlimitedExecutionToken(long taskDefinitionId);
    void InsertUnavailableExecutionToken(long taskDefinitionId);
    void InsertAvailableExecutionToken(long taskDefinitionId, int count = 1);
    void InsertExecutionToken(long taskDefinitionId, ExecInfoList tokens);
    ExecutionTokenList GetExecutionTokens(TaskId taskId);
    ExecutionTokenStatus GetExecutionTokenStatus(TaskId taskId);
    long InsertKeepAliveTaskExecution(long taskDefinitionId);

    long InsertKeepAliveTaskExecution(long taskDefinitionId, TimeSpan keepAliveInterval,
        TimeSpan keepAliveDeathThreshold);

    long InsertKeepAliveTaskExecution(long taskDefinitionId, TimeSpan keepAliveInterval,
        TimeSpan keepAliveDeathThreshold, DateTime startedAt, DateTime? completedAt);

    long InsertOverrideTaskExecution(long taskDefinitionId);
    long InsertOverrideTaskExecution(long taskDefinitionId, TimeSpan overrideThreshold);

    long InsertOverrideTaskExecution(long taskDefinitionId, TimeSpan overrideThreshold, DateTime startedAt,
        DateTime? completedAt);

    void SetTaskExecutionAsCompleted(long taskExecutionId);
    void SetLastExecutionAsDead(long taskDefinitionId);
    bool GetBlockedStatusOfLastExecution(long taskDefinitionId);
    string GetLastExecutionVersion(long taskDefinitionId);
    string GetLastExecutionHeader(long taskDefinitionId);
    void InsertUnavailableCriticalSectionToken(long taskDefinitionId, long taskExecutionId);
    void InsertAvailableCriticalSectionToken(long taskDefinitionId, long taskExecutionId);
    int GetQueueCount(long taskExecutionId);
    void InsertIntoCriticalSectionQueue(long taskDefinitionId, int queueIndex, long taskExecutionId);
    int GetCriticalSectionTokenStatus(TaskId taskId);
}