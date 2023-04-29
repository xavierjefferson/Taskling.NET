namespace Taskling.SqlServer.Blocks.Models;

public class BlockQueryItem : IBlockQueryItem
{
    public long BlockId { get; set; }
    public int Attempt { get; set; }
    public int BlockType { get; set; }
    public DateTime? FromDate { get; set; }
    public long? FromNumber { get; set; }
    public DateTime? ToDate { get; set; }
    public long? ToNumber { get; set; }
    public DateTime StartedAt { get; set; }
    public TimeSpan? OverrideThreshold { get; set; }
    public TimeSpan? KeepAliveDeathThreshold { get; set; }
    public TimeSpan? KeepAliveInterval { get; set; }
    public DateTime LastKeepAlive { get; set; }
    public byte[]? CompressedObjectData { get; set; }
    public string? ObjectData { get; set; }
    public DateTime CreatedDate { get; set; }
    public int TaskDefinitionId { get; set; }
    public string? ReferenceValue { get; set; }
    public int BlockExecutionStatus { get; set; }
}