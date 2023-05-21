using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx.Synchronous;
using Taskling.Blocks.Common;
using Taskling.Blocks.RangeBlocks;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Retries;
using Taskling.Serialization;

namespace Taskling.Blocks.ListBlocks;

public class ListBlockContextBase<TItem, THeader> : BlockContextBase
{
    private static readonly ItemStatus[] OperationalStatuses =
        { ItemStatus.Failed, ItemStatus.Pending, ItemStatus.Discarded, ItemStatus.Completed };

    private readonly bool _hasHeader;
    private readonly ILogger<ListBlockContextBase<TItem, THeader>> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IRetryService _retryService;


    protected ListBlock<TItem, THeader> _blockWithHeader;
    protected bool _completed;


    protected bool _disposed;
    protected SemaphoreSlim _getItemsSemaphore = new(1, 1);

    protected ListBlock<TItem> _headerlessBlock;


    protected IListBlockRepository _listBlockRepository;


    protected int _maxStatusReasonLength;
    protected List<IListBlockItem<TItem>> _uncommittedItems;
    protected SemaphoreSlim _uncommittedListSemaphore = new(1, 1);


    private ListBlockContextBase(IListBlockRepository listBlockRepository,
        ITaskExecutionRepository taskExecutionRepository, IRetryService retryService,
        TaskId taskId,
        long taskExecutionId,
        ListUpdateMode listUpdateMode,
        int uncommittedThreshold,
        long blockExecutionId,
        int maxStatusReasonLength, ILoggerFactory loggerFactory,
        int forcedBlockQueueId = 0) : base(taskId, blockExecutionId, taskExecutionId, retryService,
        taskExecutionRepository,
        loggerFactory.CreateLogger<BlockContextBase>(),
        forcedBlockQueueId)
    {
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<ListBlockContextBase<TItem, THeader>>();
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _listBlockRepository = listBlockRepository;
        _retryService = retryService;
        ListUpdateMode = listUpdateMode;
        UncommittedThreshold = uncommittedThreshold;
        _maxStatusReasonLength = maxStatusReasonLength;


        if (listUpdateMode != ListUpdateMode.SingleItemCommit)
            _uncommittedItems = new List<IListBlockItem<TItem>>();

        _completed = false;
    }

    public ListBlockContextBase(IListBlockRepository listBlockRepository,
        ITaskExecutionRepository taskExecutionRepository,
        TaskId taskId,
        long taskExecutionId,
        ListUpdateMode listUpdateMode,
        int uncommittedThreshold,
        ListBlock<TItem> listBlock,
        long blockExecutionId,
        int maxStatusReasonLength, IRetryService retryService, ILoggerFactory loggerFactory,
        int forcedBlockQueueId = 0) : this(listBlockRepository, taskExecutionRepository, retryService, taskId,
        taskExecutionId,
        listUpdateMode, uncommittedThreshold, blockExecutionId, maxStatusReasonLength, loggerFactory,
        forcedBlockQueueId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _headerlessBlock = listBlock;
    }

    public ListBlockContextBase(IListBlockRepository listBlockRepository,
        ITaskExecutionRepository taskExecutionRepository,
        TaskId taskId,
        long taskExecutionId,
        ListUpdateMode listUpdateMode,
        int uncommittedThreshold,
        ListBlock<TItem, THeader> listBlock,
        long blockExecutionId,
        int maxStatusReasonLength, IRetryService retryService, ILoggerFactory loggerFactory,
        int forcedBlockQueueId = 0) : this(listBlockRepository, taskExecutionRepository, retryService, taskId,
        taskExecutionId,
        listUpdateMode, uncommittedThreshold, blockExecutionId, maxStatusReasonLength, loggerFactory,
        forcedBlockQueueId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _blockWithHeader = listBlock;
        _hasHeader = true;
    }


    protected ListUpdateMode ListUpdateMode { get; }
    protected int UncommittedThreshold { get; }


    public long ListBlockId
    {
        get
        {
            if (_hasHeader)
                return _blockWithHeader.ListBlockId;

            return _headerlessBlock.ListBlockId;
        }
    }

    protected override BlockType BlockType => BlockType.List;

    protected override Func<BlockExecutionChangeStatusRequest, Task> ChangeStatusFunc =>
        _listBlockRepository.ChangeStatusAsync;

    protected virtual void Dispose(bool disposing)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (_disposed)
            return;

        if (disposing) CommitUncommittedItemsAsync().WaitAndUnwrapException();

        _disposed = true;
    }

    protected void ValidateBlockIsActive()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (_completed)
            throw new ExecutionException("The block has been marked as completed");
    }

    protected async Task UpdateItemStatusAsync(IListBlockItem<TItem> item)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        switch (ListUpdateMode)
        {
            case ListUpdateMode.SingleItemCommit:
                await CommitAsync(ListBlockId, item).ConfigureAwait(false);
                break;
            case ListUpdateMode.BatchCommitAtEnd:
                AddToUncommittedItems(item);
                break;
            case ListUpdateMode.PeriodicBatchCommit:
                await AddAndCommitIfUncommittedCountReachedAsync(item).ConfigureAwait(false);
                break;
        }
    }

    protected async Task CommitAsync(long listBlockId, IListBlockItem<TItem> item)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var singleUpdateRequest = new SingleUpdateRequest(CurrentTaskId)
        {
            ListBlockId = listBlockId,
            ListBlockItem = Convert(item)
        };

        var actionRequest = _listBlockRepository.UpdateListBlockItemAsync;
        await _retryService.InvokeWithRetryAsync(actionRequest, singleUpdateRequest).ConfigureAwait(false);
    }

    protected void AddToUncommittedItems(IListBlockItem<TItem> item)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _uncommittedListSemaphore.Wrap(() => { _uncommittedItems.Add(item); });
    }

    protected async Task AddAndCommitIfUncommittedCountReachedAsync(IListBlockItem<TItem> item)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await _uncommittedListSemaphore.WrapAsync(async () =>
        {
            _uncommittedItems.Add(item);
            if (_uncommittedItems.Count == UncommittedThreshold)
                await CommitUncommittedItemsAsync().ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    protected async Task CommitUncommittedItemsAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        List<IListBlockItem<TItem>> listToCommit = null;
        if (_uncommittedItems != null && _uncommittedItems.Any())
        {
            listToCommit = new List<IListBlockItem<TItem>>(_uncommittedItems);
            _uncommittedItems.Clear();
        }

        if (listToCommit != null && listToCommit.Any())
        {
            var batchUpdateRequest = new BatchUpdateRequest(CurrentTaskId)
            {
                ListBlockId = ListBlockId,
                ListBlockItems = Convert(listToCommit)
            };

            var actionRequest = _listBlockRepository.BatchUpdateListBlockItemsAsync;
            await _retryService.InvokeWithRetryAsync(actionRequest, batchUpdateRequest).ConfigureAwait(false);
        }
    }

    protected List<ProtoListBlockItem> Convert(List<IListBlockItem<TItem>> listBlockItems)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var items = new List<ProtoListBlockItem>();

        foreach (var listBlockItem in listBlockItems)
            items.Add(Convert(listBlockItem));

        return items;
    }

    protected ProtoListBlockItem Convert(IListBlockItem<TItem> listBlockItem)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return new ProtoListBlockItem
        {
            LastUpdated = listBlockItem.LastUpdated,
            ListBlockItemId = listBlockItem.ListBlockItemId,
            Status = listBlockItem.Status,
            StatusReason = LimitLength(listBlockItem.StatusReason, _maxStatusReasonLength),
            Step = listBlockItem.Step
        };
    }

    protected List<IListBlockItem<TItem>> Convert(IList<ProtoListBlockItem> listBlockItems)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var items = new List<IListBlockItem<TItem>>();

        foreach (var listBlockItem in listBlockItems)
            items.Add(Convert(listBlockItem));

        return items;
    }

    protected IListBlockItem<TItem> Convert(ProtoListBlockItem listBlockItem)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return new ListBlockItem<TItem>(_loggerFactory.CreateLogger<ListBlockItem<TItem>>())
        {
            LastUpdated = listBlockItem.LastUpdated,
            ListBlockItemId = listBlockItem.ListBlockItemId,
            Status = listBlockItem.Status,
            StatusReason = listBlockItem.StatusReason,
            Step = listBlockItem.Step,
            Value = JsonGenericSerializer.Deserialize<TItem>(listBlockItem.Value)
        };
    }

    protected string LimitLength(string input, int limit)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (input == null)
            return null;

        if (limit < 1)
            return input;

        if (input.Length > limit)
            return input.Substring(0, limit);

        return input;
    }

    private void SetItems(IList<IListBlockItem<TItem>> items)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (_hasHeader)
            _blockWithHeader.Items = items;

        _headerlessBlock.Items = items;
    }

    private List<T> FilterWithOperationalStatuses<T>(ItemStatus[] statuses, IEnumerable<T> z) where T : IItem
    {
        var filter = statuses.Any(x => x == ItemStatus.All) ? OperationalStatuses : statuses;

        return z.Where(x => filter.Contains(x.Status)).ToList();
    }

    private async Task<IEnumerable<IListBlockItem<TItem>>> GetItemsFromHeaderlessBlockAsync(
        params ItemStatus[] statuses)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (statuses.Length == 0)
            statuses = new[] { ItemStatus.All };

        return await _getItemsSemaphore.WrapAsync(async () =>
        {
            if (_headerlessBlock.Items == null || !_headerlessBlock.Items.Any())
            {
                var protoListBlockItems = await _listBlockRepository
                    .GetListBlockItemsAsync(CurrentTaskId, ListBlockId).ConfigureAwait(false);
                _headerlessBlock.Items = Convert(protoListBlockItems);


                foreach (var item in _headerlessBlock.Items)
                    ((ListBlockItem<TItem>)item).SetParentContext(ItemCompletedAsync, ItemFailedAsync,
                        DiscardItemAsync);
            }

            return FilterWithOperationalStatuses(statuses, _headerlessBlock.Items);
        }).ConfigureAwait(false);
    }

    private async Task<IEnumerable<IListBlockItem<TItem>>> GetItemsFromBlockWithHeaderAsync(
        params ItemStatus[] statuses)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (statuses.Length == 0)
            statuses = new[] { ItemStatus.All };
        return await _getItemsSemaphore.WrapAsync(async () =>
        {
            if (_blockWithHeader.Items == null || !_blockWithHeader.Items.Any())
            {
                var protoListBlockItems = await _listBlockRepository
                    .GetListBlockItemsAsync(CurrentTaskId, ListBlockId).ConfigureAwait(false);
                _blockWithHeader.Items = Convert(protoListBlockItems);

                foreach (var item in _blockWithHeader.Items)
                    ((ListBlockItem<TItem>)item).SetParentContext(ItemCompletedAsync, ItemFailedAsync,
                        DiscardItemAsync);
            }

            return FilterWithOperationalStatuses(statuses, _blockWithHeader.Items);
        }).ConfigureAwait(false);
    }


    public async Task FillItemsAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await _getItemsSemaphore.WrapAsync(async () =>
        {
            var protoListBlockItems = await _listBlockRepository
                .GetListBlockItemsAsync(CurrentTaskId, ListBlockId).ConfigureAwait(false);
            var listBlockItems = Convert(protoListBlockItems);
            SetItems(listBlockItems);

            foreach (var item in listBlockItems)
                ((ListBlockItem<TItem>)item).SetParentContext(ItemCompletedAsync, ItemFailedAsync, DiscardItemAsync);
        }).ConfigureAwait(false);
    }

    public IEnumerable<IListBlockItem<TItem>> GetItems(params ItemStatus[] statuses)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return GetItemsAsync(statuses).WaitAndUnwrapException();
    }

    public async Task<IEnumerable<IListBlockItem<TItem>>> GetItemsAsync(params ItemStatus[] statuses)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (_hasHeader)
            return await GetItemsFromBlockWithHeaderAsync(statuses).ConfigureAwait(false);

        return await GetItemsFromHeaderlessBlockAsync(statuses).ConfigureAwait(false);
    }

    public async Task ItemCompletedAsync(IListBlockItem<TItem> item)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        ValidateBlockIsActive();
        item.Status = ItemStatus.Completed;
        await UpdateItemStatusAsync(item).ConfigureAwait(false);
        GC.Collect();
    }

    public async Task ItemFailedAsync(IListBlockItem<TItem> item, string reason, int? step = null)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        item.StatusReason = reason;

        if (step.HasValue)
            item.Step = step;

        ValidateBlockIsActive();
        item.Status = ItemStatus.Failed;
        await UpdateItemStatusAsync(item).ConfigureAwait(false);
    }

    public async Task DiscardItemAsync(IListBlockItem<TItem> item, string reason, int? step = null)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        item.StatusReason = reason;
        if (step.HasValue)
            item.Step = step;

        ValidateBlockIsActive();
        item.Status = ItemStatus.Discarded;
        await UpdateItemStatusAsync(item).ConfigureAwait(false);
    }

    public override async Task StartAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        ValidateBlockIsActive();
        await base.StartAsync().ConfigureAwait(false);
    }

    public override async Task CompleteAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        ValidateBlockIsActive();
        await _uncommittedListSemaphore.WrapAsync(async () =>
        {
            await CommitUncommittedItemsAsync().ConfigureAwait(false);
        }).ConfigureAwait(false);


        var status = (await GetItemsAsync(ItemStatus.Failed, ItemStatus.Pending).ConfigureAwait(false)).Any()
            ? BlockExecutionStatus.Failed
            : BlockExecutionStatus.Completed;
        await ChangeBlockStatus(status).ConfigureAwait(false);
    }

    public override async Task FailedAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        ValidateBlockIsActive();
        await _uncommittedListSemaphore.WrapAsync(async () =>
        {
            await CommitUncommittedItemsAsync().ConfigureAwait(false);
        }).ConfigureAwait(false);
        await base.FailedAsync();
    }

    protected override string GetFailedErrorMessage(string message)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return $"BlockId {ListBlockId} Error: {message}";
    }

    public override async Task CompleteAsync(int itemsProcessed)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await Task.CompletedTask;
        throw new NotImplementedException();
    }


    public async Task FlushAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await _uncommittedListSemaphore.WrapAsync(async () =>
        {
            await CommitUncommittedItemsAsync().ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public void Dispose()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        Dispose(true);
    }
}