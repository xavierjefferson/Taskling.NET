using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.RangeBlocks;

public class BlockExecutionCreateRequest : BlockRequestBase
{
    public BlockExecutionCreateRequest(TaskId taskId,
        int taskExecutionId,
        BlockType blockType,
        long blockId,
        int attempt)
        : base(taskId, taskExecutionId, blockType)
    {
        BlockId = blockId;
        Attempt = attempt;
    }

    public long BlockId { get; set; }
    public int Attempt { get; set; }
}