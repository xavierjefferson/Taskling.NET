using System.Collections.Generic;

namespace Taskling.InfrastructureContracts.Blocks.ListBlocks;

public class BatchUpdateRequest
{
    public TaskId TaskId { get; set; }
    public long ListBlockId { get; set; }
    public IList<ProtoListBlockItem> ListBlockItems { get; set; }
}