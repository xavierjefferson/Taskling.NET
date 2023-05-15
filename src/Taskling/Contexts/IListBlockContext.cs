using System.Collections.Generic;
using System.Threading.Tasks;
using Taskling.Blocks.ListBlocks;

namespace Taskling.Contexts;

public interface IListBlockContextBase<T> : IBlockContext
{
    Task FillItemsAsync();
    long ListBlockId { get; }
    
    Task<IEnumerable<IListBlockItem<T>>> GetItemsAsync(params ItemStatus[] statuses);
    Task ItemFailedAsync(IListBlockItem<T> item, string reason, int? step = null);
    Task DiscardItemAsync(IListBlockItem<T> item, string reason, int? step = null);
    Task ItemCompletedAsync(IListBlockItem<T> item);
    Task FlushAsync();
    IEnumerable<IListBlockItem<T>> GetItems(params ItemStatus[] statuses);
}

public interface IListBlockContext<T> : IListBlockContextBase<T>
{
    IListBlock<T> Block { get; }
}

public interface IListBlockContext<TItem, THeader> : IListBlockContextBase<TItem>
{
    IListBlock<TItem, THeader> Block { get; }
}