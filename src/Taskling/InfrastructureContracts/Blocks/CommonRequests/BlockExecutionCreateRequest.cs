using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public class BlockExecutionCreateRequest : BlockRequestBase
{
    public BlockExecutionCreateRequest(TaskId taskId,
        long taskExecutionId,
        BlockTypeEnum blockType,
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