using Taskling.Enums;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;

namespace Taskling.InfrastructureContracts.Blocks;

public abstract class BlockRequestBase : IBlockRequest
{
    public BlockRequestBase(TaskId taskId, long taskExecutionId, BlockTypeEnum blockType, long blockExecutionId = 0)
    {
        TaskId = taskId;
        TaskExecutionId = taskExecutionId;
        BlockExecutionId = blockExecutionId;
        BlockType = blockType;
    }

    public TaskId TaskId { get; }
    public long TaskExecutionId { get; }
    public long BlockExecutionId { get; }
    public BlockTypeEnum BlockType { get; }
}