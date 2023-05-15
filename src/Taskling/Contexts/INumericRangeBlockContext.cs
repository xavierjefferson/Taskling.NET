using System.Threading.Tasks;
using Taskling.Blocks.RangeBlocks;

namespace Taskling.Contexts;

public interface INumericRangeBlockContext : IBlockContext
{
    INumericRangeBlock NumericRangeBlock { get; }
   
    Task CompleteAsync(int itemsProcessed);
  
    void Complete(int itemCountProcessed);
 
}