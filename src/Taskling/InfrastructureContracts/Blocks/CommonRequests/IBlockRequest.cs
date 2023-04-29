using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public interface IBlockRequest
{

    TaskId TaskId { get; }
    BlockType BlockType { get; }

    long BlockExecutionId { get; }
    int TaskExecutionId { get; }

}