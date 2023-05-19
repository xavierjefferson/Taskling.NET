using System;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx.Synchronous;
using Taskling.Blocks.Common;
using Taskling.Extensions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Retries;

namespace Taskling.Blocks.RangeBlocks;

public abstract class BlockContextBase
{
    private readonly ILogger<BlockContextBase> _logger;
    private readonly IRetryService _retryService;

    protected BlockContextBase(TaskId taskId, long blockExecutionId, int taskExecutionId, IRetryService retryService,
        ITaskExecutionRepository taskExecutionRepository, ILogger<BlockContextBase> logger, int forcedBlockQueueId = 0)
    {
        _retryService = retryService;
        _logger = logger;
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        CurrentTaskId = taskId;
        BlockExecutionId = blockExecutionId;
        TaskExecutionRepository = taskExecutionRepository;
        TaskExecutionId = taskExecutionId;
        ForcedBlockQueueId = forcedBlockQueueId;
    }

    protected int TaskExecutionId { get; }
    protected TaskId CurrentTaskId { get; }
    protected long BlockExecutionId { get; }
    protected ITaskExecutionRepository TaskExecutionRepository { get; }
    public int ForcedBlockQueueId { get; }
    protected abstract Func<BlockExecutionChangeStatusRequest, Task> ChangeStatusFunc { get; }

    protected abstract BlockType BlockType { get; }

    protected abstract string GetFailedErrorMessage(string message);

    public virtual async Task CompleteAsync(int itemsProcessed)
    {
        _logger.Debug("578f5dbd-bb33-4e28-89eb-afa16a2eaaa2");
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await ChangeBlockStatus(BlockExecutionStatus.Completed, itemsProcessed);
    }

    public async Task FailedAsync(string message)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await FailedAsync().ConfigureAwait(false);

        var errorMessage = GetFailedErrorMessage(message);
        var errorRequest = new TaskExecutionErrorRequest(CurrentTaskId)
        {
            TaskExecutionId = TaskExecutionId,
            TreatTaskAsFailed = false,
            Error = errorMessage
        };
        await TaskExecutionRepository.ErrorAsync(errorRequest).ConfigureAwait(false);
    }

    public virtual async Task StartAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await ChangeBlockStatus(BlockExecutionStatus.Started);
    }

    public virtual async Task CompleteAsync()
    {
        _logger.Debug("c6b4497a-724a-413e-8d48-bc48ae37f879");
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await CompleteAsync(-1).ConfigureAwait(false);
    }

    public virtual async Task FailedAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await ChangeBlockStatus(BlockExecutionStatus.Failed);
    }

    public void Complete()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        CompleteAsync().WaitAndUnwrapException();
    }

    public void Start()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        StartAsync().WaitAndUnwrapException();
    }

    public void Complete(int itemCountProcessed)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        CompleteAsync(itemCountProcessed).WaitAndUnwrapException();
    }

    public void Failed(string toString)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        FailedAsync(toString).WaitAndUnwrapException();
    }

    protected async Task ChangeBlockStatus(BlockExecutionStatus blockExecutionStatus, int? itemsProcessed = null)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var blockType = BlockType;
        var request = new BlockExecutionChangeStatusRequest(CurrentTaskId,
            TaskExecutionId,
            blockType,
            BlockExecutionId,
            blockExecutionStatus);
        if (itemsProcessed != null) request.ItemsProcessed = itemsProcessed.Value;

        await _retryService.InvokeWithRetryAsync(ChangeStatusFunc, request).ConfigureAwait(false);
    }
}