using System;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;

namespace Taskling.Blocks.Requests;

public class DateRangeBlockRequest : BlockRequest
{
    public DateRangeBlockRequest(TaskId taskId) : base(taskId)
    {
        BlockType = BlockType.DateRange;
    }

    public DateTime? RangeBegin { get; set; }
    public DateTime? RangeEnd { get; set; }
    public TimeSpan? MaxBlockRange { get; set; }
}