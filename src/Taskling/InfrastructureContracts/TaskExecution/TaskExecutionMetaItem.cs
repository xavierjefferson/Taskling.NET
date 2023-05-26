using System;
using Taskling.Enums;

namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskExecutionMetaItem
{
    public DateTime StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public TaskExecutionStatusEnum Status { get; set; }
    public string Header { get; set; }
    public Guid ReferenceValue { get; set; }
}