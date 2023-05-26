using Taskling.Enums;
using Taskling.InfrastructureContracts;

namespace Taskling.Blocks.Requests;

public class ObjectBlockRequest<T> : BlockRequest
{
    public ObjectBlockRequest(T objectData,
        int compressionThreshold, TaskId taskId) : base(taskId)
    {
        BlockType = BlockTypeEnum.Object;
        Object = objectData;
        CompressionThreshold = compressionThreshold;
    }

    public T Object { get; set; }
    public int CompressionThreshold { get; set; }
}