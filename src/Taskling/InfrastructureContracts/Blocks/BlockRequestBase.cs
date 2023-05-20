using Newtonsoft.Json;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;

namespace Taskling.InfrastructureContracts.Blocks;

public abstract class BlockRequestBase : IBlockRequest
{

    public BlockRequestBase(TaskId taskId, int taskExecutionId, BlockType blockType, long blockExecutionId = 0)
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