using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks;

public class LastBlockRequest
{
    public LastBlockRequest(TaskId taskId,
        BlockType blockType)
    {
        TaskId = taskId;
        BlockType = blockType;
    }

    public TaskId TaskId { get; set; }
    public BlockType BlockType { get; set; }
    public LastBlockOrder LastBlockOrder { get; set; }
}