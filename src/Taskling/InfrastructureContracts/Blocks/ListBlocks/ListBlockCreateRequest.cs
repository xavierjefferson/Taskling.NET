using System.Collections.Generic;
using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.ListBlocks;

public class ListBlockCreateRequest : BlockRequestBase
{
    public ListBlockCreateRequest(TaskId taskId,
        long taskExecutionId,
        List<string> serializedValues)
        : base(taskId, taskExecutionId, BlockTypeEnum.List)
    {
        SerializedValues = serializedValues;
    }

    public ListBlockCreateRequest(TaskId taskId,
        long taskExecutionId,
        List<string> serializedValues,
        string serializedHeader,
        int compressionThreshold)
        : base(taskId, taskExecutionId, BlockTypeEnum.List)
    {
        SerializedValues = serializedValues;
        SerializedHeader = serializedHeader;
        CompressionThreshold = compressionThreshold;
    }

    public List<string> SerializedValues { get; set; }
    public string SerializedHeader { get; set; }
    public int CompressionThreshold { get; set; }
}