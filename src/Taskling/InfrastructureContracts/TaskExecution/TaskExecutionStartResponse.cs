using System;
using Taskling.Enums;

namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskExecutionStartResponse : ResponseBase
{
    public TaskExecutionStartResponse()
    {
    }

    public TaskExecutionStartResponse(Guid executionTokenId,
        DateTime startedAt,
        GrantStatusEnum grantStatus)
    {
        ExecutionTokenId = executionTokenId;
        StartedAt = startedAt;
        GrantStatus = grantStatus;
    }

    public long TaskExecutionId { get; set; }
    public Guid ExecutionTokenId { get; set; }
    public DateTime StartedAt { get; set; }
    public GrantStatusEnum GrantStatus { get; set; }
    public Exception Exception { get; set; }
}