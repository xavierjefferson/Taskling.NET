using System.Threading.Tasks;
using Taskling.Blocks.Common;
using Taskling.Contexts;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Retries;

namespace Taskling.Blocks.ObjectBlocks;

public class ObjectBlockContext<T> : IObjectBlockContext<T>
{
    private readonly string _applicationName;
    private readonly IObjectBlockRepository _objectBlockRepository;
    private readonly int _taskExecutionId;
    private readonly ITaskExecutionRepository _taskExecutionRepository;
    private readonly string _taskName;

    public ObjectBlockContext(IObjectBlockRepository objectBlockRepository,
        ITaskExecutionRepository taskExecutionRepository,
        string applicationName,
        string taskName,
        int taskExecutionId,
        ObjectBlock<T> block,
        long blockExecutionId,
        int forcedBlockQueueId = 0)
    {
        _objectBlockRepository = objectBlockRepository;
        _taskExecutionRepository = taskExecutionRepository;
        Block = block;
        BlockExecutionId = blockExecutionId;
        ForcedBlockQueueId = forcedBlockQueueId;
        _applicationName = applicationName;
        _taskName = taskName;
        _taskExecutionId = taskExecutionId;
    }

    public long BlockExecutionId { get; }

    public IObjectBlock<T> Block { get; }
    public int ForcedBlockQueueId { get; }

    public async Task StartAsync()
    {
        var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
            _taskExecutionId,
            BlockType.Object,
            BlockExecutionId,
            BlockExecutionStatus.Started);

        var actionRequest = _objectBlockRepository.ChangeStatusAsync;
        await RetryService.InvokeWithRetryAsync(actionRequest, request).ConfigureAwait(false);
    }

    public async Task CompleteAsync()
    {
        await CompleteAsync(-1).ConfigureAwait(false);
    }

    public async Task FailedAsync()
    {
        var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
            _taskExecutionId,
            BlockType.Object,
            BlockExecutionId,
            BlockExecutionStatus.Failed);

        var actionRequest = _objectBlockRepository.ChangeStatusAsync;
        await RetryService.InvokeWithRetryAsync(actionRequest, request).ConfigureAwait(false);
    }

    public async Task FailedAsync(string message)
    {
        await FailedAsync().ConfigureAwait(false);

        string errorMessage = errorMessage = string.Format("BlockId {0} Error: {1}",
            Block.ObjectBlockId,
            message);

        var errorRequest = new TaskExecutionErrorRequest
        {
            TaskId = new TaskId(_applicationName, _taskName),
            TaskExecutionId = _taskExecutionId,
            TreatTaskAsFailed = false,
            Error = errorMessage
        };
        await _taskExecutionRepository.ErrorAsync(errorRequest).ConfigureAwait(false);
    }

    public async Task CompleteAsync(int itemsProcessed)
    {
        var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
            _taskExecutionId,
            BlockType.Object,
            BlockExecutionId,
            BlockExecutionStatus.Completed);
        request.ItemsProcessed = itemsProcessed;

        var actionRequest = _objectBlockRepository.ChangeStatusAsync;
        await RetryService.InvokeWithRetryAsync(actionRequest, request).ConfigureAwait(false);
    }
}