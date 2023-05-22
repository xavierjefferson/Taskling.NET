namespace Taskling.EntityFrameworkCore.Blocks.Models;

public interface IBlockQueryItem
{
    long BlockId { get; set; }
    int Attempt { get; set; }
    int BlockType { get; set; }
    string? ObjectData { get; set; }
    byte[]? CompressedObjectData { get; set; }

    DateTime? FromDate { get; set; }
    long? FromNumber { get; set; }
    DateTime? ToDate { get; set; }
    long? ToNumber { get; set; }
}