﻿using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks;

public class ForcedBlockQueueItem
{
    public BlockType BlockType { get; set; }
    public int ForcedBlockQueueId { get; set; }
}