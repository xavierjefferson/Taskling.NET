using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks;

public class QueuedForcedBlocksRequest : BlockRequestBase
{
    public QueuedForcedBlocksRequest(TaskId taskId,
        long taskExecutionId,
        BlockType blockType)
        : base(taskId, taskExecutionId, blockType)
    {
    }
}