using System.Collections.Generic;

namespace Taskling.InfrastructureContracts.Blocks.ListBlocks;

public class BatchUpdateRequest
{
    public BatchUpdateRequest(TaskId taskId)
    {
        TaskId = taskId;
    }

    public TaskId TaskId { get; }
    public long ListBlockId { get; set; }
    public IList<ProtoListBlockItem> ListBlockItems { get; set; }
}