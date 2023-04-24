using System.Collections.Generic;
using System.Threading.Tasks;

namespace Taskling.Blocks.ListBlocks;

public interface IListBlock<TItem>
{
    long ListBlockId { get; }
    int Attempt { get; }
    Task<IList<IListBlockItem<TItem>>> GetItemsAsync();
}

public interface IListBlock<TItem, THeader>
{
    long ListBlockId { get; }
    int Attempt { get; }
    THeader Header { get; }

    Task<IList<IListBlockItem<TItem>>> GetItemsAsync();
}