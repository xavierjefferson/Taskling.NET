using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Blocks.Serialization;
using Taskling.SqlServer.Models;
using TransactionScopeRetryHelper;

namespace Taskling.SqlServer.Blocks;

public class ListBlockRepository : DbOperationsService, IListBlockRepository
{
    private readonly ITaskRepository _taskRepository;

    public ListBlockRepository(ITaskRepository taskRepository, IConnectionStore connectionStore,
        IDbContextFactoryEx dbContextFactoryEx, ILoggerFactory loggerFactory) : base(connectionStore,
        dbContextFactoryEx, loggerFactory.CreateLogger<DbOperationsService>())
    {
        _taskRepository = taskRepository;
    }

    public async Task ChangeStatusAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(changeStatusRequest.TaskId))
            {
                var blockExecution = new BlockExecution { BlockExecutionId = changeStatusRequest.BlockExecutionId };
                var entityEntry = dbContext.BlockExecutions.Attach(blockExecution);
                blockExecution.BlockExecutionStatus = (int)changeStatusRequest.BlockExecutionStatus;
                switch (changeStatusRequest.BlockExecutionStatus)
                {
                    case BlockExecutionStatus.Completed:
                    case BlockExecutionStatus.Failed:
                        blockExecution.CompletedAt = DateTime.UtcNow;
                        entityEntry.Property(i => i.CompletedAt).IsModified = true;
                        break;
                    default:
                        blockExecution.StartedAt = DateTime.UtcNow;
                        entityEntry.Property(i => i.StartedAt).IsModified = true;
                        break;
                }

                //dbContext.BlockExecutions.Update(blockExecution);
                await dbContext.SaveChangesAsync().ConfigureAwait(false);
            }
        });
    }

    public async Task<IList<ProtoListBlockItem>> GetListBlockItemsAsync(TaskId taskId, long listBlockId)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var results = new List<ProtoListBlockItem>();
            using (var dbContext = await GetDbContextAsync(taskId))
            {
                var items = await dbContext.ListBlockItems.Where(i => i.BlockId == listBlockId).ToListAsync()
                    .ConfigureAwait(false);


                foreach (var item in items)
                {
                    var listBlock = new ProtoListBlockItem
                    {
                        ListBlockItemId = item.ListBlockItemId,
                        Value = SerializedValueReader.ReadValueAsString(item.Value, item.CompressedValue),
                        Status = (ItemStatus)item.Status,
                        LastUpdated = item.LastUpdated ?? DateTime.MinValue,
                        StatusReason = item.StatusReason,
                        Step = item.Step
                    };


                    results.Add(listBlock);
                }
            }

            return results;
        });
    }


    public async Task UpdateListBlockItemAsync(SingleUpdateRequest singeUpdateRequest)
    {
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(singeUpdateRequest.TaskId))
            {
                await UpdateListBlockItemsAsync(dbContext, new List<ProtoListBlockItem>
                {
                    singeUpdateRequest.ListBlockItem
                });
                await dbContext.SaveChangesAsync().ConfigureAwait(false);
            }
        });
    }

    public async Task BatchUpdateListBlockItemsAsync(BatchUpdateRequest batchUpdateRequest)
    {
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(batchUpdateRequest.TaskId))
            {
                await UpdateListBlockItemsAsync(dbContext, batchUpdateRequest.ListBlockItems);
                await dbContext.SaveChangesAsync().ConfigureAwait(false);
            }
        });
    }

    public async Task<ProtoListBlock?> GetLastListBlockAsync(LastBlockRequest lastRangeBlockRequest)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(lastRangeBlockRequest.TaskId)
                .ConfigureAwait(false);

            using (var dbContext = await GetDbContextAsync(lastRangeBlockRequest.TaskId))
            {
                var blockData = await dbContext.Blocks.OrderByDescending(i => i.BlockId).Where(i =>
                        i.TaskDefinitionId == taskDefinition.TaskDefinitionId && i.IsPhantom == false)
                    .Select(i => new { i.BlockId, i.ObjectData, i.CompressedObjectData }).FirstOrDefaultAsync()
                    .ConfigureAwait(false);
                if (blockData != null)
                {
                    var listBlock = new ProtoListBlock();
                    listBlock.ListBlockId = blockData.BlockId;
                    listBlock.Items =
                        await GetListBlockItemsAsync(lastRangeBlockRequest.TaskId, listBlock.ListBlockId)
                            .ConfigureAwait(false);
                    listBlock.Header =
                        SerializedValueReader.ReadValueAsString(blockData.ObjectData, blockData.CompressedObjectData);

                    return listBlock;
                }

                return null;
            }
        });
    }

    private async Task UpdateListBlockItemsAsync(TasklingDbContext dbContext,
        IList<ProtoListBlockItem> listBlockItems)
    {
        foreach (var listBlockItem in listBlockItems)
        {
            var entityEntry = dbContext.ListBlockItems.Attach(new ListBlockItem
            {
                ListBlockItemId = listBlockItem.ListBlockItemId,
                Status = (int)listBlockItem.Status,
                StatusReason = listBlockItem.StatusReason,
                Step = listBlockItem.Step
            });


            entityEntry.Property(i => i.Status).IsModified = true;
            entityEntry.Property(i => i.StatusReason).IsModified = true;
            entityEntry.Property(i => i.Step).IsModified = true;
        }

        await Task.CompletedTask;
    }
}