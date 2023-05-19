using Taskling.Blocks.ObjectBlocks;

namespace Taskling.Contexts;

public interface IObjectBlockContext<T> : IBlockContext
{
    IObjectBlock<T> Block { get; }
}