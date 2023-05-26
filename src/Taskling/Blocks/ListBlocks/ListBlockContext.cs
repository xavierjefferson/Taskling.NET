using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Taskling.Contexts;
using Taskling.Enums;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Retries;

namespace Taskling.Blocks.ListBlocks;

public class ListBlockContext<T> : ListBlockContextBase<T, bool>, IListBlockContext<T>, IDisposable
{
    private readonly ILogger<ListBlockContext<T>> _logger;

    public ListBlockContext(IListBlockRepository listBlockRepository,
        ITaskExecutionRepository taskExecutionRepository,
        TaskId taskId,
        long taskExecutionId,
        ListUpdateModeEnum listUpdateMode,
        int uncommittedThreshold,
        ListBlock<T> listBlock, IRetryService retryService,
        long blockExecutionId,
        int maxStatusReasonLength, ILoggerFactory loggerFactory, IServiceProvider serviceProvider,
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
        _logger = loggerFactory.CreateLogger<ListBlockContext<T>>();
        _headerlessBlock.SetParentContext(this);
    }

    public IListBlock<T> Block => _headerlessBlock;
}

public static class ServiceProviderExtensions
{
    public static ILogger<T> CreateLogger<T>(this IServiceProvider serviceProvider)
    {
        return serviceProvider.GetRequiredService<ILogger<T>>();
    }
}