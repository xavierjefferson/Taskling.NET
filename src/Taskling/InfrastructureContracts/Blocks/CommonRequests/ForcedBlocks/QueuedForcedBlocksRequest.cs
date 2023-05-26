using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks;

public class QueuedForcedBlocksRequest : BlockRequestBase
{
    public QueuedForcedBlocksRequest(TaskId taskId,
        long taskExecutionId,
        BlockTypeEnum blockType)
        : base(taskId, taskExecutionId, blockType)
    {
    }
}