namespace Taskling.SqlServer.Models;

public class TaskExecutionEvent
{
    public long TaskExecutionEventId { get; set; }
    public long TaskExecutionId { get; set; }
    public int EventType { get; set; }
    public string? Message { get; set; }
    public DateTime EventDateTime { get; set; }

    public virtual TaskExecution TaskExecution { get; set; }
}