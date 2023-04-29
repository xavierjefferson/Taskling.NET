using System.Data;
using System.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Taskling.Blocks.Common;
using Taskling.Blocks.RangeBlocks;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;

namespace Taskling.SqlServer.Blocks;

public class RangeBlockRepository : DbOperationsService, IRangeBlockRepository
{
    private readonly ITaskRepository _taskRepository;

    public RangeBlockRepository(ITaskRepository taskRepository)
    {
        _taskRepository = taskRepository;
    }

    public async Task ChangeStatusAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
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
        return await RetryHelper.WithRetry(async (transactionScope) =>
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
                        lastRangeBlockRequest.BlockType);
                }
            }

            return null;

        });




    }


    private async Task ChangeStatusOfDateRangeExecutionAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        await ChangeStatusOfNumericRangeExecutionAsync(changeStatusRequest);
        //try
        //{
        //    using (var connection = await CreateNewConnectionAsync(changeStatusRequest.TaskId).ConfigureAwait(false))
        //    {
        //        var command = connection.CreateCommand();
        //        command.CommandTimeout = ConnectionStore.Instance.GetConnection(changeStatusRequest.TaskId)
        //            .QueryTimeoutSeconds;
        //        if (changeStatusRequest.BlockExecutionStatus == BlockExecutionStatus.Completed ||
        //            changeStatusRequest.BlockExecutionStatus == BlockExecutionStatus.Failed)
        //            command.CommandText = BlockExecutionQueryBuilder.SetRangeBlockExecutionAsCompleted;
        //        else
        //            command.CommandText = BlockExecutionQueryBuilder.SetBlockExecutionStatusToStarted;

        //        command.Parameters.Add("@BlockExecutionId", SqlDbType.BigInt).Value =
        //            changeStatusRequest.BlockExecutionId;
        //        command.Parameters.Add("@BlockExecutionStatus", SqlDbType.Int).Value =
        //            (int)changeStatusRequest.BlockExecutionStatus;
        //        command.Parameters.Add("@ItemsCount", SqlDbType.Int).Value = changeStatusRequest.ItemsProcessed;

        //        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        //    }
        //}
        //catch (SqlException sqlEx)
        //{
        //    if (TransientErrorDetector.IsTransient(sqlEx))
        //        throw new TransientException("A transient exception has occurred", sqlEx);

        //    throw;
        //}
    }

    private async Task ChangeStatusOfNumericRangeExecutionAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        await RetryHelper.WithRetry(async (transactionScope) =>
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