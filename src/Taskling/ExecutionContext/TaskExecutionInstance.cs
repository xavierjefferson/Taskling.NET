using System;
using Taskling.InfrastructureContracts;

namespace Taskling.ExecutionContext;

public class TaskExecutionInstance
{
    public TaskExecutionInstance(TaskId taskId)
    {
        TaskId = taskId;
    }

    public TaskId TaskId { get; }
    public int TaskExecutionId { get; set; }

    public DateTime StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public Guid ExecutionTokenId { get; set; }
}