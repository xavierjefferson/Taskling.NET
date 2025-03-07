﻿using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.AncilliaryServices;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CleanUp;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.EntityFrameworkCore.TaskExecution;

public class CleanUpRepository : DbOperationsService, ICleanUpRepository
{
    private readonly ILogger<CleanUpRepository> _logger;
    private readonly ITaskRepository _taskRepository;

    public CleanUpRepository(ITaskRepository taskRepository, 
        ILogger<CleanUpRepository> logger,
        IDbContextFactoryEx dbContextFactoryEx, ILoggerFactory loggerFactory) : base(dbContextFactoryEx, loggerFactory.CreateLogger<DbOperationsService>())
    {
        _taskRepository = taskRepository;
        _logger = logger;
    }

    public async Task<bool> CleanOldDataAsync(CleanUpRequest cleanUpRequest)
    {
        var lastCleaned =
            await _taskRepository.GetLastTaskCleanUpTimeAsync(cleanUpRequest.TaskId).ConfigureAwait(false);
        var periodSinceLastClean = DateTime.UtcNow - lastCleaned;

        if (periodSinceLastClean > cleanUpRequest.TimeSinceLastCleaningThreshold)
        {
            await _taskRepository.SetLastCleanedAsync(cleanUpRequest.TaskId).ConfigureAwait(false);
            var taskDefinition =
                await _taskRepository.EnsureTaskDefinitionAsync(cleanUpRequest.TaskId).ConfigureAwait(false);
            await CleanListItemsAsync(cleanUpRequest.TaskId, taskDefinition.TaskDefinitionId,
                cleanUpRequest.ListItemDateThreshold).ConfigureAwait(false);
            await CleanOldDataAsync(cleanUpRequest.TaskId, taskDefinition.TaskDefinitionId,
                cleanUpRequest.GeneralDateThreshold).ConfigureAwait(false);
            return true;
        }

        return false;
    }

    private async Task CleanListItemsAsync(TaskId taskId, long taskDefinitionId, DateTime listItemDateThreshold)
    {
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))
            {
                dbContext.ListBlockItems.RemoveRange(dbContext.ListBlockItems.Where(i =>
                    i.Block.TaskDefinitionId == taskDefinitionId && i.Block.CreatedDate <= listItemDateThreshold));
                await dbContext.SaveChangesAsync().ConfigureAwait(false);
            }
        });
    }

    private async Task CleanOldDataAsync(TaskId taskId, long taskDefinitionId, DateTime generalDateThreshold)
    {
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(taskId))
            {
                dbContext.BlockExecutions.RemoveRange(dbContext.BlockExecutions.Where(i =>
                    i.Block.TaskDefinitionId == taskDefinitionId && i.Block.CreatedDate < generalDateThreshold));
                dbContext.ForcedBlockQueues.RemoveRange(dbContext.ForcedBlockQueues.Where(i =>
                    i.Block.TaskDefinitionId == taskDefinitionId && i.Block.CreatedDate < generalDateThreshold));
                dbContext.Blocks.RemoveRange(dbContext.Blocks.Where(i =>
                    i.TaskDefinitionId == taskDefinitionId && i.CreatedDate < generalDateThreshold));
                dbContext.TaskExecutionEvents.RemoveRange(dbContext.TaskExecutionEvents.Where(i =>
                    i.TaskExecution.TaskDefinitionId == taskDefinitionId &&
                    i.TaskExecution.StartedAt < generalDateThreshold));
                dbContext.TaskExecutions.RemoveRange(dbContext.TaskExecutions.Where(i =>
                    i.TaskDefinitionId == taskDefinitionId && i.StartedAt < generalDateThreshold));
                dbContext.ForcedBlockQueues.RemoveRange(
                    dbContext.ForcedBlockQueues.Where(i => i.ForcedDate < generalDateThreshold));
                await dbContext.SaveChangesAsync().ConfigureAwait(false);
            }
        });
    }
}