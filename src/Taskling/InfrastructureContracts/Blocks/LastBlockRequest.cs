using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks;

public class LastBlockRequest
{
    public LastBlockRequest(TaskId taskId,
        BlockTypeEnum blockType)
    {
        TaskId = taskId;
        BlockType = blockType;
    }

    public TaskId TaskId { get; }
    public BlockTypeEnum BlockType { get; set; }
    public LastBlockOrderEnum LastBlockOrder { get; set; }
}