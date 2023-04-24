using System;

namespace Taskling.Blocks.RangeBlocks;

public interface IDateRangeBlock
{
    long RangeBlockId { get; }
    int Attempt { get; set; }
    DateTime StartDate { get; }
    DateTime EndDate { get; }
}