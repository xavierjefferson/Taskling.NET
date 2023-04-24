﻿using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Taskling.Blocks.Common;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Retries;
using Taskling.Serialization;

namespace Taskling.Blocks.ListBlocks;

public class ListBlockContextBase<TItem, THeader>
{
    #region .: Protected Fields :.

    protected IListBlockRepository _listBlockRepository;
    protected ITaskExecutionRepository _taskExecutionRepository;
    protected string _applicationName;
    protected string _taskName;
    protected int _taskExecutionId;
    protected int _maxStatusReasonLength;
    protected SemaphoreSlim _uncommittedListSemaphore = new(1, 1);
    protected SemaphoreSlim _getItemsSemaphore = new(1, 1);
    protected List<IListBlockItem<TItem>> _uncommittedItems;
    protected bool _completed;

    private readonly bool _hasHeader;

    #endregion .: Protected Fields :.


    #region .: Constructor :.

    public ListBlockContextBase(IListBlockRepository listBlockRepository,
        ITaskExecutionRepository taskExecutionRepository,
        string applicationName,
        string taskName,
        int taskExecutionId,
        ListUpdateMode listUpdateMode,
        int uncommittedThreshold,
        ListBlock<TItem> listBlock,
        long blockExecutionId,
        int maxStatusReasonLength,
        int forcedBlockQueueId = 0)
    {
        _listBlockRepository = listBlockRepository;
        _taskExecutionRepository = taskExecutionRepository;
        _headerlessBlock = listBlock;
        BlockExecutionId = blockExecutionId;
        ListUpdateMode = listUpdateMode;
        ForcedBlockQueueId = forcedBlockQueueId;
        UncommittedThreshold = uncommittedThreshold;
        _applicationName = applicationName;
        _taskName = taskName;
        _taskExecutionId = taskExecutionId;
        _maxStatusReasonLength = maxStatusReasonLength;

        if (listUpdateMode != ListUpdateMode.SingleItemCommit)
            _uncommittedItems = new List<IListBlockItem<TItem>>();

        _completed = false;
    }

    public ListBlockContextBase(IListBlockRepository listBlockRepository,
        ITaskExecutionRepository taskExecutionRepository,
        string applicationName,
        string taskName,
        int taskExecutionId,
        ListUpdateMode listUpdateMode,
        int uncommittedThreshold,
        ListBlock<TItem, THeader> listBlock,
        long blockExecutionId,
        int maxStatusReasonLength,
        int forcedBlockQueueId = 0)
    {
        _listBlockRepository = listBlockRepository;
        _taskExecutionRepository = taskExecutionRepository;
        _blockWithHeader = listBlock;
        BlockExecutionId = blockExecutionId;
        ListUpdateMode = listUpdateMode;
        ForcedBlockQueueId = forcedBlockQueueId;
        UncommittedThreshold = uncommittedThreshold;
        _applicationName = applicationName;
        _taskName = taskName;
        _taskExecutionId = taskExecutionId;
        _maxStatusReasonLength = maxStatusReasonLength;

        if (listUpdateMode != ListUpdateMode.SingleItemCommit)
            _uncommittedItems = new List<IListBlockItem<TItem>>();

        _completed = false;
        _hasHeader = true;
    }

    #endregion .: Constructor :.


    #region .: Protected Properties :.

    protected long BlockExecutionId { get; set; }
    protected ListUpdateMode ListUpdateMode { get; set; }
    protected int UncommittedThreshold { get; set; }

    #endregion .: Protected Properties :.


    #region .: Public Propeties :.

    public long ListBlockId
    {
        get
        {
            if (_hasHeader)
                return _blockWithHeader.ListBlockId;

            return _headerlessBlock.ListBlockId;
        }
    }

    protected ListBlock<TItem> _headerlessBlock;

    protected ListBlock<TItem, THeader> _blockWithHeader;

    public int ForcedBlockQueueId { get; }

    #endregion .: Public Propeties :.


    #region .: Protected Methods :.

    protected bool disposed;

    protected virtual void Dispose(bool disposing)
    {
        if (disposed)
            return;

        if (disposing) Task.Run(() => CommitUncommittedItemsAsync());

        disposed = true;
    }

    protected void ValidateBlockIsActive()
    {
        if (_completed)
            throw new ExecutionException("The block has been marked as completed");
    }

    protected async Task SetStatusAsFailedAsync()
    {
        var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
            _taskExecutionId,
            BlockType.List,
            BlockExecutionId,
            BlockExecutionStatus.Failed);

        var actionRequest = _listBlockRepository.ChangeStatusAsync;
        await RetryService.InvokeWithRetryAsync(actionRequest, request).ConfigureAwait(false);
    }

    protected async Task UpdateItemStatusAsync(IListBlockItem<TItem> item)
    {
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
        var singleUpdateRequest = new SingleUpdateRequest
        {
            TaskId = new TaskId(_applicationName, _taskName),
            ListBlockId = listBlockId,
            ListBlockItem = Convert(item)
        };

        var actionRequest = _listBlockRepository.UpdateListBlockItemAsync;
        await RetryService.InvokeWithRetryAsync(actionRequest, singleUpdateRequest).ConfigureAwait(false);
    }

    protected void AddToUncommittedItems(IListBlockItem<TItem> item)
    {
        _uncommittedListSemaphore.Wait();
        try
        {
            _uncommittedItems.Add(item);
        }
        finally
        {
            _uncommittedListSemaphore.Release();
        }
    }

    protected async Task AddAndCommitIfUncommittedCountReachedAsync(IListBlockItem<TItem> item)
    {
        await _uncommittedListSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            _uncommittedItems.Add(item);
            if (_uncommittedItems.Count == UncommittedThreshold)
                await CommitUncommittedItemsAsync().ConfigureAwait(false);
        }
        finally
        {
            _uncommittedListSemaphore.Release();
        }
    }

    protected async Task CommitUncommittedItemsAsync()
    {
        List<IListBlockItem<TItem>> listToCommit = null;
        if (_uncommittedItems != null && _uncommittedItems.Any())
        {
            listToCommit = new List<IListBlockItem<TItem>>(_uncommittedItems);
            _uncommittedItems.Clear();
        }

        if (listToCommit != null && listToCommit.Any())
        {
            var batchUpdateRequest = new BatchUpdateRequest
            {
                TaskId = new TaskId(_applicationName, _taskName),
                ListBlockId = ListBlockId,
                ListBlockItems = Convert(listToCommit)
            };

            var actionRequest = _listBlockRepository.BatchUpdateListBlockItemsAsync;
            await RetryService.InvokeWithRetryAsync(actionRequest, batchUpdateRequest).ConfigureAwait(false);
        }
    }

    protected List<ProtoListBlockItem> Convert(List<IListBlockItem<TItem>> listBlockItems)
    {
        var items = new List<ProtoListBlockItem>();

        foreach (var listBlockItem in listBlockItems)
            items.Add(Convert(listBlockItem));

        return items;
    }

    protected ProtoListBlockItem Convert(IListBlockItem<TItem> listBlockItem)
    {
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
        var items = new List<IListBlockItem<TItem>>();

        foreach (var listBlockItem in listBlockItems)
            items.Add(Convert(listBlockItem));

        return items;
    }

    protected IListBlockItem<TItem> Convert(ProtoListBlockItem listBlockItem)
    {
        return new ListBlockItem<TItem>
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
        if (_hasHeader)
            _blockWithHeader.Items = items;

        _headerlessBlock.Items = items;
    }

    private async Task<IEnumerable<IListBlockItem<TItem>>> GetItemsFromHeaderlessBlockAsync(
        params ItemStatus[] statuses)
    {
        if (statuses.Length == 0)
            statuses = new[] { ItemStatus.All };

        await _getItemsSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_headerlessBlock.Items == null || !_headerlessBlock.Items.Any())
            {
                var protoListBlockItems = await _listBlockRepository
                    .GetListBlockItemsAsync(new TaskId(_applicationName, _taskName), ListBlockId).ConfigureAwait(false);
                _headerlessBlock.Items = Convert(protoListBlockItems);

                foreach (var item in _headerlessBlock.Items)
                    ((ListBlockItem<TItem>)item).SetParentContext(ItemCompleteAsync, ItemFailedAsync, DiscardItemAsync);
            }

            if (statuses.Any(x => x == ItemStatus.All))
                return _headerlessBlock.Items.Where(x =>
                    x.Status == ItemStatus.Failed || x.Status == ItemStatus.Pending ||
                    x.Status == ItemStatus.Discarded || x.Status == ItemStatus.Completed).ToList();

            return _headerlessBlock.Items.Where(x => statuses.Contains(x.Status)).ToList();
        }
        finally
        {
            _getItemsSemaphore.Release();
        }
    }

    private async Task<IEnumerable<IListBlockItem<TItem>>> GetItemsFromBlockWithHeaderAsync(
        params ItemStatus[] statuses)
    {
        if (statuses.Length == 0)
            statuses = new[] { ItemStatus.All };

        await _getItemsSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_blockWithHeader.Items == null || !_blockWithHeader.Items.Any())
            {
                var protoListBlockItems = await _listBlockRepository
                    .GetListBlockItemsAsync(new TaskId(_applicationName, _taskName), ListBlockId).ConfigureAwait(false);
                _blockWithHeader.Items = Convert(protoListBlockItems);

                foreach (var item in _blockWithHeader.Items)
                    ((ListBlockItem<TItem>)item).SetParentContext(ItemCompleteAsync, ItemFailedAsync, DiscardItemAsync);
            }

            if (statuses.Any(x => x == ItemStatus.All))
                return _blockWithHeader.Items.Where(x =>
                    x.Status == ItemStatus.Failed || x.Status == ItemStatus.Pending ||
                    x.Status == ItemStatus.Discarded || x.Status == ItemStatus.Completed).ToList();

            return _blockWithHeader.Items.Where(x => statuses.Contains(x.Status)).ToList();
        }
        finally
        {
            _getItemsSemaphore.Release();
        }
    }

    #endregion .: Private/Protected Methods :.


    #region .: Public Methods :.

    public async Task FillItemsAsync()
    {
        await _getItemsSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            var protoListBlockItems = await _listBlockRepository
                .GetListBlockItemsAsync(new TaskId(_applicationName, _taskName), ListBlockId).ConfigureAwait(false);
            var listBlockItems = Convert(protoListBlockItems);
            SetItems(listBlockItems);

            foreach (var item in listBlockItems)
                ((ListBlockItem<TItem>)item).SetParentContext(ItemCompleteAsync, ItemFailedAsync, DiscardItemAsync);
        }
        finally
        {
            _getItemsSemaphore.Release();
        }
    }

    public async Task<IEnumerable<IListBlockItem<TItem>>> GetItemsAsync(params ItemStatus[] statuses)
    {
        if (_hasHeader)
            return await GetItemsFromBlockWithHeaderAsync(statuses).ConfigureAwait(false);

        return await GetItemsFromHeaderlessBlockAsync(statuses).ConfigureAwait(false);
    }

    public async Task ItemCompleteAsync(IListBlockItem<TItem> item)
    {
        ValidateBlockIsActive();
        item.Status = ItemStatus.Completed;
        await UpdateItemStatusAsync(item).ConfigureAwait(false);
    }

    public async Task ItemFailedAsync(IListBlockItem<TItem> item, string reason, int? step = null)
    {
        item.StatusReason = reason;

        if (step.HasValue)
            item.Step = step;

        ValidateBlockIsActive();
        item.Status = ItemStatus.Failed;
        await UpdateItemStatusAsync(item).ConfigureAwait(false);
    }

    public async Task DiscardItemAsync(IListBlockItem<TItem> item, string reason, int? step = null)
    {
        item.StatusReason = reason;
        if (step.HasValue)
            item.Step = step;

        ValidateBlockIsActive();
        item.Status = ItemStatus.Discarded;
        await UpdateItemStatusAsync(item).ConfigureAwait(false);
    }

    public async Task StartAsync()
    {
        ValidateBlockIsActive();
        var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
            _taskExecutionId,
            BlockType.List,
            BlockExecutionId,
            BlockExecutionStatus.Started);

        var actionRequest = _listBlockRepository.ChangeStatusAsync;
        await RetryService.InvokeWithRetryAsync(actionRequest, request).ConfigureAwait(false);
    }

    public async Task CompleteAsync()
    {
        ValidateBlockIsActive();
        await _uncommittedListSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            await CommitUncommittedItemsAsync().ConfigureAwait(false);
        }
        finally
        {
            _uncommittedListSemaphore.Release();
        }

        var status = BlockExecutionStatus.Completed;
        if ((await GetItemsAsync(ItemStatus.Failed, ItemStatus.Pending).ConfigureAwait(false)).Any())
            status = BlockExecutionStatus.Failed;

        var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
            _taskExecutionId,
            BlockType.List,
            BlockExecutionId,
            status);

        var actionRequest = _listBlockRepository.ChangeStatusAsync;
        await RetryService.InvokeWithRetryAsync(actionRequest, request).ConfigureAwait(false);
    }

    public async Task FailedAsync()
    {
        ValidateBlockIsActive();

        await _uncommittedListSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            await CommitUncommittedItemsAsync().ConfigureAwait(false);
        }
        finally
        {
            _uncommittedListSemaphore.Release();
        }

        await SetStatusAsFailedAsync().ConfigureAwait(false);
    }

    public async Task FailedAsync(string message)
    {
        await FailedAsync().ConfigureAwait(false);

        var errorMessage = string.Format("BlockId {0} Error: {1}", ListBlockId, message);
        var errorRequest = new TaskExecutionErrorRequest
        {
            TaskId = new TaskId(_applicationName, _taskName),
            TaskExecutionId = _taskExecutionId,
            TreatTaskAsFailed = false,
            Error = errorMessage
        };
        await _taskExecutionRepository.ErrorAsync(errorRequest).ConfigureAwait(false);
    }

    public async Task<IEnumerable<TItem>> GetItemValuesAsync(params ItemStatus[] statuses)
    {
        if (statuses.Length == 0)
            statuses = new[] { ItemStatus.All };

        return (await GetItemsAsync(statuses).ConfigureAwait(false)).Select(x => x.Value);
    }

    public async Task FlushAsync()
    {
        await _uncommittedListSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            await CommitUncommittedItemsAsync().ConfigureAwait(false);
        }
        finally
        {
            _uncommittedListSemaphore.Release();
        }
    }

    public void Dispose()
    {
        Dispose(true);
    }

    #endregion .: Public Methods :.
}