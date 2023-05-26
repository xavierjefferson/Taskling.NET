using System;
using Taskling.Enums;
using Taskling.InfrastructureContracts;

namespace Taskling.Blocks.Requests;

public class DateRangeBlockRequest : BlockRequest
{
    public DateRangeBlockRequest(TaskId taskId) : base(taskId)
    {
        BlockType = BlockTypeEnum.DateRange;
    }

    public DateTime? RangeBegin { get; set; }
    public DateTime? RangeEnd { get; set; }
    public TimeSpan? MaxBlockRange { get; set; }
}