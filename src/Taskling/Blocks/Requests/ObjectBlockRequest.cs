using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;

namespace Taskling.Blocks.Requests;

public class ObjectBlockRequest<T> : BlockRequest
{
    public ObjectBlockRequest(T objectData,
        int compressionThreshold, TaskId taskId) : base(taskId)
    {
        BlockType = BlockType.Object;
        Object = objectData;
        CompressionThreshold = compressionThreshold;
    }

    public T Object { get; set; }
    public int CompressionThreshold { get; set; }
}