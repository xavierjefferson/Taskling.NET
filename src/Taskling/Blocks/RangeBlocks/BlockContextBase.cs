using System;
using System.Threading.Tasks;
using Nito.AsyncEx.Synchronous;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Retries;

namespace Taskling.Blocks.RangeBlocks;

public abstract class BlockContextBase
{ 
    protected BlockContextBase(TaskId taskId, long blockExecutionId, int taskExecutionId,
        ITaskExecutionRepository taskExecutionRepository, int forcedBlockQueueId = 0)
    {
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
        await ChangeBlockStatus(BlockExecutionStatus.Completed, itemsProcessed);
    }

    public async Task FailedAsync(string message)
    {
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
        await ChangeBlockStatus(BlockExecutionStatus.Started);
    }

    public virtual async Task CompleteAsync()
    {
        await CompleteAsync(-1).ConfigureAwait(false);
    }
    public virtual async Task FailedAsync()
    {
        await ChangeBlockStatus(BlockExecutionStatus.Failed);
    }
    public void Complete()
    {
        CompleteAsync().WaitAndUnwrapException();
    }
    public void Start()
    {
        StartAsync().WaitAndUnwrapException();
    }

    public void Complete(int itemCountProcessed)
    {
        CompleteAsync(itemCountProcessed).WaitAndUnwrapException();
    }

    public void Failed(string toString)
    {
        FailedAsync(toString).WaitAndUnwrapException();
    }

    protected async Task ChangeBlockStatus(BlockExecutionStatus blockExecutionStatus, int? itemsProcessed = null)
    {
        var blockType = BlockType;
        var request = new BlockExecutionChangeStatusRequest(CurrentTaskId,
            TaskExecutionId,
            blockType,
            BlockExecutionId,
            blockExecutionStatus);
        if (itemsProcessed != null) request.ItemsProcessed = itemsProcessed.Value;

        await RetryService.InvokeWithRetryAsync(ChangeStatusFunc, request).ConfigureAwait(false);
    }
}