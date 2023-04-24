using System.Threading.Tasks;
using Taskling.Blocks.RangeBlocks;

namespace Taskling.Contexts;

public interface IDateRangeBlockContext : IBlockContext
{
    IDateRangeBlock DateRangeBlock { get; }
    int ForcedBlockQueueId { get; }
    Task CompleteAsync(int itemsProcessed);
}