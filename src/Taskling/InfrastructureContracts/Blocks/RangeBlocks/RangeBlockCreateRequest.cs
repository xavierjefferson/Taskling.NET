using System;
using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.RangeBlocks;

public class RangeBlockCreateRequest : BlockRequestBase
{
    public RangeBlockCreateRequest(TaskId taskId,
        long taskExecutionId,
        DateTime fromDate,
        DateTime toDate)
        : base(taskId, taskExecutionId, BlockTypeEnum.DateRange)
    {
        From = fromDate.Ticks;
        To = toDate.Ticks;
    }

    public RangeBlockCreateRequest(TaskId taskId,
        long taskExecutionId,
        long from,
        long to)
        : base(taskId, taskExecutionId, BlockTypeEnum.NumericRange)
    {
        From = from;
        To = to;
    }

    public long From { get; set; }
    public long To { get; set; }
}