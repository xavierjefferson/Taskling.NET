namespace Taskling.Blocks.RangeBlocks;

public interface INumericRangeBlock
{
    long RangeBlockId { get; }
    int Attempt { get; set; }
    long StartNumber { get; }
    long EndNumber { get; }
}