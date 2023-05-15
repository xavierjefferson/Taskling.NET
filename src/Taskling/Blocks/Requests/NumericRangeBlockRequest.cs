using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;

namespace Taskling.Blocks.Requests;

public class NumericRangeBlockRequest : BlockRequest
{
    public NumericRangeBlockRequest(TaskId taskId) : base(taskId)
    {
        BlockType = BlockType.NumericRange;
    }

    public long? RangeBegin { get; set; }
    public long? RangeEnd { get; set; }
    public long? BlockSize { get; set; }
}