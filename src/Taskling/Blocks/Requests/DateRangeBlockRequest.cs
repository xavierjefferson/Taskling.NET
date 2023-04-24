using System;
using Taskling.Blocks.Common;

namespace Taskling.Blocks.Requests;

public class DateRangeBlockRequest : BlockRequest
{
    public DateRangeBlockRequest()
    {
        BlockType = BlockType.DateRange;
    }

    public DateTime? RangeBegin { get; set; }
    public DateTime? RangeEnd { get; set; }
    public TimeSpan? MaxBlockRange { get; set; }
}