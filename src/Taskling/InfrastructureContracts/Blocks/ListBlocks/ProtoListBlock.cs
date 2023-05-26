using System.Collections.Generic;

namespace Taskling.InfrastructureContracts.Blocks.ListBlocks;

public class ProtoListBlock
{
    public long ListBlockId { get; set; }
    public int Attempt { get; set; }
    public string Header { get; set; }
    public bool IsForcedBlock { get; set; }
    public long ForcedBlockQueueId { get; set; }
    public IList<ProtoListBlockItem> Items { get; set; }
}