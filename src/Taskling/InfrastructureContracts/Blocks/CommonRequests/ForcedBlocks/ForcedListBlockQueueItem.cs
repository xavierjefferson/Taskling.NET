using Taskling.InfrastructureContracts.Blocks.ListBlocks;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks;

public class ForcedListBlockQueueItem : ForcedBlockQueueItem
{
    public ProtoListBlock ListBlock { get; set; }
}