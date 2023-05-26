using System.Collections.Generic;
using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks;

public class DequeueForcedBlocksRequest : BlockRequestBase
{
    public DequeueForcedBlocksRequest(TaskId taskId,
        long taskExecutionId,
        BlockTypeEnum blockType,
        List<long> forcedBlockQueueIds)
        : base(taskId, taskExecutionId, blockType)
    {
        ForcedBlockQueueIds = forcedBlockQueueIds;
    }

    public List<long> ForcedBlockQueueIds { get; set; }
}