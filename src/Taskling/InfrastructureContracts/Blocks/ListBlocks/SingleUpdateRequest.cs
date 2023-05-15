namespace Taskling.InfrastructureContracts.Blocks.ListBlocks;

public class SingleUpdateRequest
{
    public SingleUpdateRequest(TaskId taskId)
    {
        TaskId = taskId;
    }

    public TaskId TaskId { get; }
    public long ListBlockId { get; set; }
    public ProtoListBlockItem ListBlockItem { get; set; }
}