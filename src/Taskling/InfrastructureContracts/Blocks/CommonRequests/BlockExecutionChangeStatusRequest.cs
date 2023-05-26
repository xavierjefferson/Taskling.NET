using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public class BlockExecutionChangeStatusRequest : BlockRequestBase
{
    public BlockExecutionChangeStatusRequest(TaskId taskId,
        long taskExecutionId,
        BlockTypeEnum blockType,
        long blockExecutionId,
        BlockExecutionStatusEnum blockExecutionStatus)
        : base(taskId, taskExecutionId, blockType, blockExecutionId)
    {
        BlockExecutionStatus = blockExecutionStatus;
    }

    public BlockExecutionStatusEnum BlockExecutionStatus { get; set; }
    public int ItemsProcessed { get; set; }
}