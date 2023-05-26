namespace Taskling.EntityFrameworkCore.Models;

public class ForcedBlockQueue
{
    public long ForcedBlockQueueId { get; set; }
    public long BlockId { get; set; }
    public DateTime ForcedDate { get; set; }
    public string? ForcedBy { get; set; }
    public string? ProcessingStatus { get; set; }

    public virtual Block? Block { get; set; }
}