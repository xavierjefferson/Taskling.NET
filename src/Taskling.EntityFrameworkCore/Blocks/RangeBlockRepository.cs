using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;
using Taskling.Blocks.RangeBlocks;
using Taskling.EntityFrameworkCore.AncilliaryServices;
using Taskling.Extensions;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.EntityFrameworkCore.Blocks;

public class RangeBlockRepository : DbOperationsService, IRangeBlockRepository
{
    private readonly ILogger<RangeBlockRepository> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ITaskRepository _taskRepository;

    public RangeBlockRepository(ITaskRepository taskRepository, IConnectionStore connectionStore,
        ILogger<RangeBlockRepository> logger,
        IDbContextFactoryEx dbContextFactoryEx, ILoggerFactory loggerFactory) : base(connectionStore,
        dbContextFactoryEx, loggerFactory.CreateLogger<DbOperationsService>())
    {
        _taskRepository = taskRepository;
        _logger = logger;
        _loggerFactory = loggerFactory;
    }

    public async Task ChangeStatusAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        _logger.LogDebug($"Called {nameof(ChangeStatusAsync)} where blocktype={changeStatusRequest.BlockType}");
        switch (changeStatusRequest.BlockType)
        {
            case BlockType.DateRange:
                await ChangeStatusOfDateRangeExecutionAsync(changeStatusRequest).ConfigureAwait(false);
                break;
            case BlockType.NumericRange:
                await ChangeStatusOfNumericRangeExecutionAsync(changeStatusRequest).ConfigureAwait(false);
                break;
            default:
                throw new NotSupportedException("This range type is not supported");
        }
    }

    public async Task<RangeBlock?> GetLastRangeBlockAsync(LastBlockRequest lastRangeBlockRequest)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(lastRangeBlockRequest.TaskId)
                .ConfigureAwait(false);
            using (var dbContext = await GetDbContextAsync(lastRangeBlockRequest.TaskId))
            {
                var blockQueryable = dbContext.Blocks.Where(i =>
                    i.IsPhantom == false && i.TaskDefinitionId == taskDefinition.TaskDefinitionId);
                switch (lastRangeBlockRequest.BlockType)
                {
                    case BlockType.NumericRange:
                        switch (lastRangeBlockRequest.LastBlockOrder)
                        {
                            default:
                            case LastBlockOrder.LastCreated:
                                blockQueryable = blockQueryable.OrderByDescending(i => i.CreatedDate);
                                break;
                            case LastBlockOrder.MaxRangeEndValue:
                                blockQueryable = blockQueryable.OrderByDescending(i => i.ToNumber);
                                break;
                            case LastBlockOrder.MaxRangeStartValue:
                                blockQueryable = blockQueryable.OrderByDescending(i => i.FromNumber);
                                break;
                        }

                        break;
                    case BlockType.DateRange:
                        switch (lastRangeBlockRequest.LastBlockOrder)
                        {
                            default:
                            case LastBlockOrder.LastCreated:
                                blockQueryable = blockQueryable.OrderByDescending(i => i.CreatedDate);
                                break;
                            case LastBlockOrder.MaxRangeEndValue:
                                blockQueryable = blockQueryable.OrderByDescending(i => i.ToDate);
                                break;
                            case LastBlockOrder.MaxRangeStartValue:
                                blockQueryable = blockQueryable.OrderByDescending(i => i.FromDate);
                                break;
                        }

                        break;
                    default:
                        throw new ArgumentException("An invalid BlockType was supplied: " +
                                                    lastRangeBlockRequest.BlockType);
                }

                var block = await blockQueryable.FirstOrDefaultAsync().ConfigureAwait(false);

                if (block != null)
                {
                    var rangeBlockId = block.BlockId;
                    long rangeBegin;
                    long rangeEnd;

                    if (lastRangeBlockRequest.BlockType == BlockType.DateRange)
                    {
                        rangeBegin = block.FromDate.Value.Ticks; //reader.GetDateTime("FromDate").Ticks; 
                        rangeEnd = block.ToDate.Value.Ticks; //reader.GetDateTime("ToDate").Ticks;
                    }
                    else
                    {
                        rangeBegin = block.FromNumber.Value;
                        rangeEnd = block.ToNumber.Value;
                    }

                    return new RangeBlock(rangeBlockId, 0, rangeBegin, rangeEnd,
                        lastRangeBlockRequest.BlockType, _loggerFactory.CreateLogger<RangeBlock>());
                }
            }

            return null;
        });
    }


    private async Task ChangeStatusOfDateRangeExecutionAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        await ChangeStatusOfNumericRangeExecutionAsync(changeStatusRequest);
    }

    private async Task ChangeStatusOfNumericRangeExecutionAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        _logger.LogDebug($"Called {nameof(ChangeStatusOfNumericRangeExecutionAsync)} to change blockexecutionid {changeStatusRequest.BlockExecutionId}, status={changeStatusRequest.BlockExecutionStatus}, itemsCount={changeStatusRequest.ItemsProcessed}");
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(changeStatusRequest.TaskId).ConfigureAwait(false))
            {
                var blockExecution = await dbContext.BlockExecutions.FirstOrDefaultAsync(i =>
                    i.BlockExecutionId == changeStatusRequest.BlockExecutionId).ConfigureAwait(false);
                if (blockExecution != null)
                {
                    blockExecution.BlockExecutionStatus = (int)changeStatusRequest.BlockExecutionStatus;
                    switch (changeStatusRequest.BlockExecutionStatus)
                    {
                        case BlockExecutionStatus.Completed:
                        case BlockExecutionStatus.Failed:
                            blockExecution.ItemsCount = changeStatusRequest.ItemsProcessed;
                            blockExecution.CompletedAt = DateTime.UtcNow;
                            break;
                        default:
                            blockExecution.StartedAt = DateTime.UtcNow;
                            break;
                    }

                    dbContext.BlockExecutions.Update(blockExecution);
                    await dbContext.SaveChangesAsync().ConfigureAwait(false);
                }
            }
        });
    }
}