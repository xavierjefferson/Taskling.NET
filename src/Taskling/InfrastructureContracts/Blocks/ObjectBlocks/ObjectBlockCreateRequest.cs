using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.ObjectBlocks;

public class ObjectBlockCreateRequest<T> : BlockRequestBase
{
    public ObjectBlockCreateRequest(TaskId taskId,
        long taskExecutionId,
        T objectData,
        int compressionThreshold)
        : base(taskId, taskExecutionId, BlockTypeEnum.Object)
    {
        Object = objectData;
        CompressionThreshold = compressionThreshold;
    }

    public T Object { get; set; }
    public int CompressionThreshold { get; set; }
}