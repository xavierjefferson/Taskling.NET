using System;
using Microsoft.Extensions.Logging;
using Taskling.Contexts;
using Taskling.Enums;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Retries;

namespace Taskling.Blocks.ListBlocks;

public class ListBlockContext<TItem, THeader> : ListBlockContextBase<TItem, THeader>, IListBlockContext<TItem, THeader>,
    IDisposable
{
    private readonly ILogger<ListBlockContext<TItem, THeader>> _logger;

    public ListBlockContext(IListBlockRepository listBlockRepository,
        ITaskExecutionRepository taskExecutionRepository,
        TaskId taskId,
        long taskExecutionId,
        ListUpdateModeEnum listUpdateMode,
        int uncommittedThreshold,
        ListBlock<TItem, THeader> listBlock,
        long blockExecutionId,
        int maxStatusReasonLength, IServiceProvider serviceProvider,
        ILoggerFactory loggerFactory,
        IRetryService retryService,
        long forcedBlockQueueId = 0)
        : base(listBlockRepository,
            taskExecutionRepository,
            taskId,
            taskExecutionId,
            listUpdateMode,
            uncommittedThreshold,
            listBlock,
            blockExecutionId,
            maxStatusReasonLength, retryService, loggerFactory,
            forcedBlockQueueId)
    {
        _logger = loggerFactory.CreateLogger<ListBlockContext<TItem, THeader>>();
        _blockWithHeader.SetParentContext(this);
    }

    public IListBlock<TItem, THeader> Block => _blockWithHeader;
}