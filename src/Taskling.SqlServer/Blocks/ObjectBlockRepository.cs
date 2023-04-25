using System.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Taskling.Blocks.Common;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Blocks.Serialization;

namespace Taskling.SqlServer.Blocks;

public class ObjectBlockRepository : DbOperationsService, IObjectBlockRepository
{
    private readonly ITaskRepository _taskRepository;

    public ObjectBlockRepository(ITaskRepository taskRepository)
    {
        _taskRepository = taskRepository;
    }

    public async Task ChangeStatusAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        await ChangeStatusOfExecutionAsync(changeStatusRequest).ConfigureAwait(false);
    }

    public async Task<ObjectBlock<T>> GetLastObjectBlockAsync<T>(LastBlockRequest lastRangeBlockRequest)
    {
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(lastRangeBlockRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var dbContext = await GetDbContextAsync(lastRangeBlockRequest.TaskId).ConfigureAwait(false))
            {
                var blockData = await dbContext.Blocks.Where(i => i.TaskDefinitionId == taskDefinition.TaskDefinitionId && i.IsPhantom == false)
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
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }

        return null;
    }


    private async Task ChangeStatusOfExecutionAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        try
        {
            using (var dbContext = await GetDbContextAsync(changeStatusRequest.TaskId))
            {
                var blockExecution = await dbContext.BlockExecutions
                    .FirstOrDefaultAsync(i => i.BlockExecutionId == changeStatusRequest.BlockExecutionId)
                    .ConfigureAwait(false);
                if (blockExecution != null)
                {
                    blockExecution.BlockExecutionStatus = (int)changeStatusRequest.BlockExecutionStatus;
                    if (changeStatusRequest.BlockExecutionStatus == BlockExecutionStatus.Completed ||
                        changeStatusRequest.BlockExecutionStatus == BlockExecutionStatus.Failed)
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
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }
    }
}