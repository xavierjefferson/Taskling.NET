using System.Threading.Tasks;
using Taskling.Blocks.Common;
using Taskling.Contexts;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Retries;

namespace Taskling.Blocks.RangeBlocks;

public class RangeBlockContext : IDateRangeBlockContext, INumericRangeBlockContext
{
    private readonly string _applicationName;

    private readonly RangeBlock _block;
    private readonly IRangeBlockRepository _rangeBlockRepository;
    private readonly int _taskExecutionId;
    private readonly ITaskExecutionRepository _taskExecutionRepository;
    private readonly string _taskName;

    public RangeBlockContext(IRangeBlockRepository rangeBlockRepository,
        ITaskExecutionRepository taskExecutionRepository,
        string applicationName,
        string taskName,
        int taskExecutionId,
        RangeBlock rangeBlock,
        long blockExecutionId,
        int forcedBlockQueueId = 0)
    {
        _rangeBlockRepository = rangeBlockRepository;
        _taskExecutionRepository = taskExecutionRepository;
        _block = rangeBlock;
        BlockExecutionId = blockExecutionId;
        ForcedBlockQueueId = forcedBlockQueueId;
        _applicationName = applicationName;
        _taskName = taskName;
        _taskExecutionId = taskExecutionId;
    }

    public long BlockExecutionId { get; }

    public IDateRangeBlock DateRangeBlock => _block;

    public int ForcedBlockQueueId { get; }

    public async Task StartAsync()
    {
        var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
            _taskExecutionId,
            _block.RangeType,
            BlockExecutionId,
            BlockExecutionStatus.Started);

        var actionRequest = _rangeBlockRepository.ChangeStatusAsync;
        await RetryService.InvokeWithRetryAsync(actionRequest, request).ConfigureAwait(false);
    }

    public async Task CompleteAsync()
    {
        await CompleteAsync(-1).ConfigureAwait(false);
    }

    public async Task CompleteAsync(int itemsProcessed)
    {
        var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
            _taskExecutionId,
            _block.RangeType,
            BlockExecutionId,
            BlockExecutionStatus.Completed);
        request.ItemsProcessed = itemsProcessed;

        var actionRequest = _rangeBlockRepository.ChangeStatusAsync;
        await RetryService.InvokeWithRetryAsync(actionRequest, request).ConfigureAwait(false);
    }

    public async Task FailedAsync()
    {
        var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
            _taskExecutionId,
            _block.RangeType,
            BlockExecutionId,
            BlockExecutionStatus.Failed);

        var actionRequest = _rangeBlockRepository.ChangeStatusAsync;
        await RetryService.InvokeWithRetryAsync(actionRequest, request).ConfigureAwait(false);
    }

    public async Task FailedAsync(string message)
    {
        await FailedAsync().ConfigureAwait(false);

        var errorMessage = string.Empty;
        if (_block.RangeType == BlockType.DateRange)
            errorMessage = string.Format("BlockId {0} From: {1} To: {2} Error: {3}",
                _block.RangeBlockId,
                _block.RangeBeginAsDateTime().ToString("yyyy-MM-dd HH:mm:ss"),
                _block.RangeEndAsDateTime().ToString("yyyy-MM-dd HH:mm:ss"),
                message);
        else
            errorMessage = string.Format("BlockId {0} From: {1} To: {2} Error: {3}",
                _block.RangeBlockId,
                _block.RangeBeginAsLong(),
                _block.RangeEndAsLong(),
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

    public INumericRangeBlock NumericRangeBlock => _block;
}