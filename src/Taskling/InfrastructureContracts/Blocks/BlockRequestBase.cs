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
    [JsonProperty(Order = 900)]
    public TaskId TaskId { get; }
    [JsonProperty(Order = 500)]
    public int TaskExecutionId { get; }
    public long BlockExecutionId { get; }
    [JsonProperty(Order = 100)]
    public BlockType BlockType { get; }
}