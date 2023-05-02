using System;
using System.Collections.Generic;
using Nito.AsyncEx.Synchronous;
using Taskling.Contexts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.Blocks.ListBlocks;

public class ListBlockContext<TItem, THeader> : ListBlockContextBase<TItem, THeader>, IListBlockContext<TItem, THeader>,
    IDisposable
{
    public ListBlockContext(IListBlockRepository listBlockRepository,
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
        : base(listBlockRepository,
            taskExecutionRepository,
            applicationName,
            taskName,
            taskExecutionId,
            listUpdateMode,
            uncommittedThreshold,
            listBlock,
            blockExecutionId,
            maxStatusReasonLength,
            forcedBlockQueueId)
    {
        _blockWithHeader.SetParentContext(this);
    }

    public IListBlock<TItem, THeader> Block => _blockWithHeader;

    public IEnumerable<IListBlockItem<TItem>> GetItems(params ItemStatus[] statuses)
    {
        return GetItemsAsync(statuses).WaitAndUnwrapException();
    }

    public void Complete()
    {
        CompleteAsync().WaitAndUnwrapException();
    }

    public void Failed(string toString)
    {
        FailedAsync(toString).WaitAndUnwrapException();
    }

    public void Start()
    {
        StartAsync().WaitAndUnwrapException();
    }
}