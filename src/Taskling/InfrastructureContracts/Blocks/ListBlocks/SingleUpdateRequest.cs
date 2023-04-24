namespace Taskling.InfrastructureContracts.Blocks.ListBlocks;

public class SingleUpdateRequest
{
    public TaskId TaskId { get; set; }
    public long ListBlockId { get; set; }
    public ProtoListBlockItem ListBlockItem { get; set; }
}