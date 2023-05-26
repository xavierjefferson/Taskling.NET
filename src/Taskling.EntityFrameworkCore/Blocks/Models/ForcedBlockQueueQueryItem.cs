namespace Taskling.EntityFrameworkCore.Blocks.Models;

public class ForcedBlockQueueQueryItem
{
    public DateTime? FromDate { get; set; }
    public DateTime? ToDate { get; set; }
    public int BlockType { get; set; }
    public string? ProcessingStatus { get; set; }
    public long TaskDefinitionId { get; set; }
    public long BlockId { get; set; }
    public long ForcedBlockQueueId { get; set; }
    public long? FromNumber { get; set; }
    public long? ToNumber { get; set; }
    public int Attempt { get; set; }
    public string? ObjectData { get; set; }
    public byte[]? CompressedObjectData { get; set; }
}