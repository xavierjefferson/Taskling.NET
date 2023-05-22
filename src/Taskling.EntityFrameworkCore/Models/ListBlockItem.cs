namespace Taskling.EntityFrameworkCore.Models;

public class ListBlockItem
{
    public long ListBlockItemId { get; set; }
    public long BlockId { get; set; }
    public string? Value { get; set; }
    public byte[]? CompressedValue { get; set; }
    public int Status { get; set; }
    public DateTime? Timestamp { get; set; }
    public DateTime? LastUpdated { get; set; }
    public string? StatusReason { get; set; }
    public int? Step { get; set; }

    public virtual Block? Block { get; set; }
}