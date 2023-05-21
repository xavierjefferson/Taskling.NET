using System;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx.Synchronous;

namespace Taskling.Blocks.ListBlocks;

public class ListBlockItem<T> : IListBlockItem<T>
{
    private readonly ILogger<ListBlockItem<T>> _logger;
    private Func<IListBlockItem<T>, string, int?, Task> _discardItem;
    private Func<IListBlockItem<T>, Task> _itemComplete;
    private Func<IListBlockItem<T>, string, int?, Task> _itemFailed;

    public ListBlockItem(ILogger<ListBlockItem<T>> logger)
    {
        _logger = logger;
    }

    public long ListBlockItemId { get; set; }
    public T Value { get; set; }
    public ItemStatus Status { get; set; }
    public string StatusReason { get; set; }
    public DateTime LastUpdated { get; set; }
    public int? Step { get; set; }

    public async Task CompleteAsync()
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

    public void Failed(string message)
    {
        FailedAsync(message).WaitAndUnwrapException();
    }

    public void Discarded(string message)
    {
        DiscardedAsync(message).WaitAndUnwrapException();
    }

    public void Complete()
    {
        CompleteAsync().WaitAndUnwrapException();
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