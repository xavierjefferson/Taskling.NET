using System.Threading.Tasks;
using Taskling.Blocks.RangeBlocks;

namespace Taskling.Contexts;

public interface INumericRangeBlockContext : IBlockContext
{
    INumericRangeBlock NumericRangeBlock { get; }
    int ForcedBlockQueueId { get; }
    Task CompleteAsync(int itemsProcessed);
    void Start();
    void Complete(int itemCountProcessed);
    void Failed(string toString);
}