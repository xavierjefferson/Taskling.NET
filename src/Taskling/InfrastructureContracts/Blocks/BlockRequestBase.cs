using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks;

public class BlockRequestBase
{
    public BlockRequestBase(TaskId taskId, int taskExecutionId, BlockType blockType)
    {
        TaskId = taskId;
        TaskExecutionId = taskExecutionId;
        BlockType = blockType;
    }

    public BlockRequestBase(TaskId taskId, int taskExecutionId, BlockType blockType, long blockExecutionId)
    {
        TaskId = taskId;
        TaskExecutionId = taskExecutionId;
        BlockExecutionId = blockExecutionId;
        BlockType = blockType;
    }

    public TaskId TaskId { get; }
    public int TaskExecutionId { get; }
    public long BlockExecutionId { get; }
    public BlockType BlockType { get; }
}