using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks;

public class ForcedBlockQueueItem
{
    public BlockTypeEnum BlockType { get; set; }
    public long ForcedBlockQueueId { get; set; }
}