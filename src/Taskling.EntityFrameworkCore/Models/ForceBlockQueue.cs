namespace Taskling.EntityFrameworkCore.Models;

public class ForceBlockQueue
{
    public int ForceBlockQueueId { get; set; }
    public long BlockId { get; set; }
    public DateTime ForcedDate { get; set; }
    public string? ForcedBy { get; set; }
    public string? ProcessingStatus { get; set; }

    public virtual Block? Block { get; set; }
}