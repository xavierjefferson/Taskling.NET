using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.ObjectBlocks;
using Taskling.EntityFrameworkCore.AncilliaryServices;
using Taskling.EntityFrameworkCore.Blocks.Serialization;
using Taskling.Enums;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.EntityFrameworkCore.Blocks;

public class ObjectBlockRepository : DbOperationsService, IObjectBlockRepository
{
    private readonly ITaskRepository _taskRepository;

    public ObjectBlockRepository(ITaskRepository taskRepository, IConnectionStore connectionStore,
        IDbContextFactoryEx dbContextFactoryEx, ILoggerFactory loggerFactory) : base(connectionStore,
        dbContextFactoryEx, loggerFactory.CreateLogger<DbOperationsService>())
    {
        _taskRepository = taskRepository;
    }

    public async Task ChangeStatusAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        await ChangeStatusOfExecutionAsync(changeStatusRequest).ConfigureAwait(false);
    }

    public async Task<ObjectBlock<T>?> GetLastObjectBlockAsync<T>(LastBlockRequest lastRangeBlockRequest)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(lastRangeBlockRequest.TaskId)
                .ConfigureAwait(false);
            using (var dbContext = await GetDbContextAsync(lastRangeBlockRequest.TaskId).ConfigureAwait(false))
            {
                var blockData = await dbContext.Blocks.Where(i =>
                        i.TaskDefinitionId == taskDefinition.TaskDefinitionId && i.IsPhantom == false)
                    .OrderByDescending(i => i.BlockId)
                    .Select(i => new { i.ObjectData, i.BlockId, i.CompressedObjectData }).FirstOrDefaultAsync()
                    .ConfigureAwait(false);
                if (blockData != null)
                {
                    var objectData =
                        SerializedValueReader.ReadValue<T>(blockData.ObjectData, blockData.CompressedObjectData);

                    return new ObjectBlock<T>
                    {
                        Object = objectData,
                        ObjectBlockId = blockData.BlockId
                    };
                }
            }

            return null;
        });
    }

    private async Task ChangeStatusOfExecutionAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(changeStatusRequest.TaskId))
            {
                var blockExecution = await dbContext.BlockExecutions
                    .FirstOrDefaultAsync(i => i.BlockExecutionId == changeStatusRequest.BlockExecutionId)
                    .ConfigureAwait(false);
                if (blockExecution != null)
                {
                    blockExecution.BlockExecutionStatus = (int)changeStatusRequest.BlockExecutionStatus;
                    if (changeStatusRequest.BlockExecutionStatus == BlockExecutionStatusEnum.Completed ||
                        changeStatusRequest.BlockExecutionStatus == BlockExecutionStatusEnum.Failed)
                    {
                        blockExecution.ItemsCount = changeStatusRequest.ItemsProcessed;
                        blockExecution.CompletedAt = DateTime.UtcNow;
                    }
                    else
                    {
                        blockExecution.StartedAt = DateTime.UtcNow;
                    }

                    await dbContext.SaveChangesAsync().ConfigureAwait(false);
                }
            }
        });
    }
}