using System.Threading.Tasks;
using Taskling.Blocks.ObjectBlocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;

namespace Taskling.InfrastructureContracts.Blocks;

public interface IObjectBlockRepository
{
    Task ChangeStatusAsync(BlockExecutionChangeStatusRequest changeStatusRequest);
    Task<ObjectBlock<T>> GetLastObjectBlockAsync<T>(LastBlockRequest lastRangeBlockRequest);
}