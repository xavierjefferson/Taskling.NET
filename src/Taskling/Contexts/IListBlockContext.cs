using System.Collections.Generic;
using System.Threading.Tasks;
using Taskling.Blocks.ListBlocks;

namespace Taskling.Contexts;

public interface IListBlockContext<T> : IBlockContext
{
    IListBlock<T> Block { get; }
    long ListBlockId { get; }
    int ForcedBlockQueueId { get; }
    Task<IEnumerable<IListBlockItem<T>>> GetItemsAsync(params ItemStatus[] statuses);
    Task ItemCompletedAsync(IListBlockItem<T> item);
    Task ItemFailedAsync(IListBlockItem<T> item, string reason, int? step = null);
    Task DiscardItemAsync(IListBlockItem<T> item, string reason, int? step = null);
    Task<IEnumerable<T>> GetItemValuesAsync(params ItemStatus[] statuses);
    Task FlushAsync();
}

public interface IListBlockContext<TItem, THeader> : IBlockContext
{
    IListBlock<TItem, THeader> Block { get; }
    long ListBlockId { get; }
    int ForcedBlockQueueId { get; }
    IEnumerable<IListBlockItem<TItem>> GetItems(params ItemStatus[] statuses);
    Task<IEnumerable<IListBlockItem<TItem>>> GetItemsAsync(params ItemStatus[] statuses);
    Task ItemCompletedAsync(IListBlockItem<TItem> item);
    Task ItemFailedAsync(IListBlockItem<TItem> item, string reason, int? step = null);
    Task DiscardItemAsync(IListBlockItem<TItem> item, string reason, int? step = null);
    Task<IEnumerable<TItem>> GetItemValuesAsync(params ItemStatus[] statuses);
    Task FlushAsync();
    void Complete();
    void Failed(string toString);
    void Start();
}