using System.Threading.Tasks;
using Taskling.Blocks.RangeBlocks;

namespace Taskling.Contexts;

public interface IDateRangeBlockContext : IBlockContext
{
    IDateRangeBlock DateRangeBlock { get; }
    Task CompleteAsync(int itemsProcessed);

    void Complete(int itemCountProcessed);
}