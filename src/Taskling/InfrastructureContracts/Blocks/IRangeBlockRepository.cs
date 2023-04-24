using System.Threading.Tasks;
using Taskling.Blocks.RangeBlocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;

namespace Taskling.InfrastructureContracts.Blocks;

public interface IRangeBlockRepository
{
    Task ChangeStatusAsync(BlockExecutionChangeStatusRequest changeStatusRequest);
    Task<RangeBlock> GetLastRangeBlockAsync(LastBlockRequest lastRangeBlockRequest);
}