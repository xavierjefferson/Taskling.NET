using System;
using System.Threading.Tasks;

namespace Taskling.Blocks.ListBlocks;

public class ListBlockItem<T> : IListBlockItem<T>
{
    private Func<IListBlockItem<T>, string, int?, Task> _discardItem;
    private Func<IListBlockItem<T>, Task> _itemComplete;
    private Func<IListBlockItem<T>, string, int?, Task> _itemFailed;

    public string ListBlockItemId { get; set; }
    public T Value { get; set; }
    public ItemStatus Status { get; set; }
    public string StatusReason { get; set; }
    public DateTime LastUpdated { get; set; }
    public int? Step { get; set; }

    public async Task CompletedAsync()
    {
        await _itemComplete(this).ConfigureAwait(false);
    }

    public async Task FailedAsync(string message)
    {
        await _itemFailed(this, message, null).ConfigureAwait(false);
    }

    public async Task DiscardedAsync(string message)
    {
        await _discardItem(this, message, null).ConfigureAwait(false);
    }

    internal void SetParentContext(Func<IListBlockItem<T>, Task> itemComplete,
        Func<IListBlockItem<T>, string, int?, Task> itemFailed,
        Func<IListBlockItem<T>, string, int?, Task> discardItem)
    {
        _itemComplete = itemComplete;
        _itemFailed = itemFailed;
        _discardItem = discardItem;
    }
}