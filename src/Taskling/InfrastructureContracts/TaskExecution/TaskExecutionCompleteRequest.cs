using System;

namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskExecutionCompleteRequest : RequestBase
{
    public TaskExecutionCompleteRequest(TaskId taskId, int taskExecutionId, Guid executionTokenId)
        : base(taskId, taskExecutionId)
    {
        ExecutionTokenId = executionTokenId;
    }

    public Guid ExecutionTokenId { get; set; }
    public bool Failed { get; set; }
}