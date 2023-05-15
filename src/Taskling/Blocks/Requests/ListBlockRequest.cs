using System.Collections.Generic;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.InfrastructureContracts;

namespace Taskling.Blocks.Requests;

public class ListBlockRequest : BlockRequest
{
    public ListBlockRequest(TaskId taskId) : base(taskId)
    {
        BlockType = BlockType.List;
    }

    public List<string> SerializedValues { get; set; }
    public string SerializedHeader { get; set; }
    public int CompressionThreshold { get; set; }
    public int MaxStatusReasonLength { get; set; }
    public int MaxBlockSize { get; set; }
    public ListUpdateMode ListUpdateMode { get; set; }
    public int UncommittedItemsThreshold { get; set; }
}