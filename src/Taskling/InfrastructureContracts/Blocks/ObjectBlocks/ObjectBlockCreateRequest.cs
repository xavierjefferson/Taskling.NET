using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.ObjectBlocks;

public class ObjectBlockCreateRequest<T> : BlockRequestBase
{
    public ObjectBlockCreateRequest(TaskId taskId,
        int taskExecutionId,
        T objectData,
        int compressionThreshold)
        : base(taskId, taskExecutionId, BlockType.Object)
    {
        Object = objectData;
        CompressionThreshold = compressionThreshold;
    }

    public T Object { get; set; }
    public int CompressionThreshold { get; set; }
}