namespace Taskling.EntityFrameworkCore.Models;

public class TaskExecution
{
    public TaskExecution()
    {
        BlockExecutions = new HashSet<BlockExecution>();
        TaskExecutionEvents = new HashSet<TaskExecutionEvent>();
    }

    public long TaskExecutionId { get; set; }
    public long TaskDefinitionId { get; set; }
    public DateTime StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public DateTime LastKeepAlive { get; set; }
    public string? ServerName { get; set; }
    public int TaskDeathMode { get; set; }
    public TimeSpan? OverrideThreshold { get; set; }
    public TimeSpan? KeepAliveInterval { get; set; }
    public TimeSpan? KeepAliveDeathThreshold { get; set; }
    public int FailedTaskRetryLimit { get; set; }
    public int DeadTaskRetryLimit { get; set; }
    public Guid ReferenceValue { get; set; }
    public bool Failed { get; set; }
    public bool Blocked { get; set; }
    public string? TasklingVersion { get; set; }
    public string? ExecutionHeader { get; set; }

    public virtual TaskDefinition TaskDefinition { get; set; }
    public virtual ICollection<BlockExecution> BlockExecutions { get; set; }
    public virtual ICollection<TaskExecutionEvent> TaskExecutionEvents { get; set; }
}