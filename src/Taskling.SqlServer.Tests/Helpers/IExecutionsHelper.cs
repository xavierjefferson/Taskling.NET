using System;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.Tokens.Executions;

namespace Taskling.SqlServer.Tests.Helpers;

public interface IExecutionsHelper
{
    TaskId CurrentTaskId { get; }
    void DeleteRecordsOfApplication(string applicationName);
    void SetKeepAlive(int taskExecutionId);
    void SetKeepAlive(int taskExecutionId, DateTime keepAliveDateTime);
    DateTime GetLastKeepAlive(int taskDefinitionId);
    GetLastEventResponse GetLastEvent(int taskDefinitionId);
    int InsertTask(string applicationName, string taskName);
    int InsertTask(TaskId taskId);
    void InsertUnlimitedExecutionToken(int taskDefinitionId);
    void InsertUnavailableExecutionToken(int taskDefinitionId);
    void InsertAvailableExecutionToken(int taskDefinitionId, int count = 1);
    void InsertExecutionToken(int taskDefinitionId, ExecInfoList tokens);
    ExecutionTokenList GetExecutionTokens(TaskId taskId);
    ExecutionTokenStatus GetExecutionTokenStatus(TaskId taskId);
    int InsertKeepAliveTaskExecution(int taskDefinitionId);

    int InsertKeepAliveTaskExecution(int taskDefinitionId, TimeSpan keepAliveInterval,
        TimeSpan keepAliveDeathThreshold);

    int InsertKeepAliveTaskExecution(int taskDefinitionId, TimeSpan keepAliveInterval,
        TimeSpan keepAliveDeathThreshold, DateTime startedAt, DateTime? completedAt);

    int InsertOverrideTaskExecution(int taskDefinitionId);
    int InsertOverrideTaskExecution(int taskDefinitionId, TimeSpan overrideThreshold);

    int InsertOverrideTaskExecution(int taskDefinitionId, TimeSpan overrideThreshold, DateTime startedAt,
        DateTime? completedAt);

    void SetTaskExecutionAsCompleted(int taskExecutionId);
    void SetLastExecutionAsDead(int taskDefinitionId);
    bool GetBlockedStatusOfLastExecution(int taskDefinitionId);
    string GetLastExecutionVersion(int taskDefinitionId);
    string GetLastExecutionHeader(int taskDefinitionId);
    void InsertUnavailableCriticalSectionToken(int taskDefinitionId, int taskExecutionId);
    void InsertAvailableCriticalSectionToken(int taskDefinitionId, int taskExecutionId);
    int GetQueueCount(int taskExecutionId);
    void InsertIntoCriticalSectionQueue(int taskDefinitionId, int queueIndex, int taskExecutionId);
    int GetCriticalSectionTokenStatus(TaskId taskId);
}