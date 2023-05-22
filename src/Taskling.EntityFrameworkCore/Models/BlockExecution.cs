namespace Taskling.EntityFrameworkCore.Models;

public class BlockExecution
{
    public long BlockExecutionId { get; set; }
    public long TaskExecutionId { get; set; }
    public long BlockId { get; set; }
    public DateTime? StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public DateTime CreatedAt { get; set; }
    public int Attempt { get; set; }
    public int? ItemsCount { get; set; }
    public int BlockExecutionStatus { get; set; }
    public virtual Block? Block { get; set; }
    public virtual TaskExecution? TaskExecution { get; set; }
}