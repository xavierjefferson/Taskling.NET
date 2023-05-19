using System;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;

namespace Taskling.Blocks.RangeBlocks;

public class RangeBlock : IDateRangeBlock, INumericRangeBlock
{
    private readonly ILogger<RangeBlock> _logger;

    public RangeBlock(long rangeBlockId,
        int attempt,
        long rangeBegin,
        long rangeEnd,
        BlockType blockType, ILogger<RangeBlock> logger)
    {
        _logger = logger;

        RangeBlockId = rangeBlockId;
        Attempt = attempt;
        RangeBegin = rangeBegin;
        RangeEnd = rangeEnd;
        RangeType = blockType;
    }

    private long RangeBegin { get; }
    private long RangeEnd { get; }

    public BlockType RangeType { get; set; }

    public long RangeBlockId { get; set; }
    public int Attempt { get; set; }

    public DateTime StartDate => RangeBeginAsDateTime();

    public DateTime EndDate => RangeEndAsDateTime();

    public long StartNumber => RangeBegin;

    public long EndNumber => RangeEnd;

    public bool IsEmpty()
    {
        return RangeBlockId == 0 && RangeBegin == 0 && RangeEnd == 0;
    }

    public int RangeBeginAsInt()
    {
        return (int)RangeBegin;
    }

    public int RangeBeginAsInt(int defaultIfEmptyValue)
    {
        if (IsEmpty())
            return defaultIfEmptyValue;
        return (int)RangeBegin;
    }

    public long RangeBeginAsLong()
    {
        return RangeBegin;
    }

    public long RangeBeginAsLong(long defaultIfEmptyValue)
    {
        if (IsEmpty())
            return defaultIfEmptyValue;

        return RangeBegin;
    }

    public DateTime RangeBeginAsDateTime()
    {
        return new DateTime(RangeBegin);
    }

    public DateTime RangeBeginAsDateTime(DateTime defaultIfEmptyValue)
    {
        if (IsEmpty())
            return defaultIfEmptyValue;

        return new DateTime(RangeBegin);
    }

    public int RangeEndAsInt()
    {
        return (int)RangeEnd;
    }

    public int RangeEndAsInt(int defaultIfEmptyValue)
    {
        if (IsEmpty())
            return defaultIfEmptyValue;

        return (int)RangeEnd;
    }

    public long RangeEndAsLong()
    {
        return RangeEnd;
    }

    public long RangeEndAsLong(long defaultIfEmptyValue)
    {
        if (IsEmpty())
            return defaultIfEmptyValue;

        return RangeEnd;
    }

    public DateTime RangeEndAsDateTime()
    {
        return new DateTime(RangeEnd);
    }

    public DateTime RangeEndAsDateTime(DateTime defaultIfEmptyValue)
    {
        if (IsEmpty())
            return defaultIfEmptyValue;

        return new DateTime(RangeEnd);
    }
}