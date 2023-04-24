using System.Collections.Generic;
using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks;

public class DequeueForcedBlocksRequest : BlockRequestBase
{
    public DequeueForcedBlocksRequest(TaskId taskId,
        int taskExecutionId,
        BlockType blockType,
        List<int> forcedBlockQueueIds)
        : base(taskId, taskExecutionId, blockType)
    {
        ForcedBlockQueueIds = forcedBlockQueueIds;
    }

    public List<int> ForcedBlockQueueIds { get; set; }
}