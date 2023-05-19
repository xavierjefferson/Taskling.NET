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
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await _itemComplete(this).ConfigureAwait(false);
    }

    public async Task FailedAsync(string message)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await _itemFailed(this, message, null).ConfigureAwait(false);
    }

    public async Task DiscardedAsync(string message)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await _discardItem(this, message, null).ConfigureAwait(false);
    }

    public void Failed(string message)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        FailedAsync(message).WaitAndUnwrapException();
    }

    public void Discarded(string message)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        DiscardedAsync(message).WaitAndUnwrapException();
    }

    public void Complete()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        CompleteAsync().WaitAndUnwrapException();
    }

    internal void SetParentContext(Func<IListBlockItem<T>, Task> itemComplete,
        Func<IListBlockItem<T>, string, int?, Task> itemFailed,
        Func<IListBlockItem<T>, string, int?, Task> discardItem)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _itemComplete = itemComplete;
        _itemFailed = itemFailed;
        _discardItem = discardItem;
    }
}