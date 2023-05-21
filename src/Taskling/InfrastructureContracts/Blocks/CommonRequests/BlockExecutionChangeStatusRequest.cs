using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public class BlockExecutionChangeStatusRequest : BlockRequestBase
{
    public BlockExecutionChangeStatusRequest(TaskId taskId,
        long taskExecutionId,
        BlockType blockType,
        long blockExecutionId,
        BlockExecutionStatus blockExecutionStatus)
        : base(taskId, taskExecutionId, blockType, blockExecutionId)
    {
        BlockExecutionStatus = blockExecutionStatus;
    }

    public BlockExecutionStatus BlockExecutionStatus { get; set; }
    public int ItemsProcessed { get; set; }
}