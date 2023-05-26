using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public interface IBlockRequest
{
    TaskId TaskId { get; }
    BlockTypeEnum BlockType { get; }

    long BlockExecutionId { get; }
    long TaskExecutionId { get; }
}