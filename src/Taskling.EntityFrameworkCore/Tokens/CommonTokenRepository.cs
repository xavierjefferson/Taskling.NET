using Microsoft.EntityFrameworkCore;
using Taskling.EntityFrameworkCore.Models;
using Taskling.Tasks;

namespace Taskling.EntityFrameworkCore.Tokens;

public class CommonTokenRepository : ICommonTokenRepository
{
    public async Task AcquireRowLockAsync(long taskDefinitionId, long taskExecutionId,
        TasklingDbContext dbContext)
    {
        try
        {
            var exampleEntity =
                dbContext.TaskDefinitions.Attach(new TaskDefinition { TaskDefinitionId = taskDefinitionId });
            exampleEntity.Entity.HoldLockTaskExecutionId = taskExecutionId;
            exampleEntity.Property(i => i.HoldLockTaskExecutionId).IsModified = true;
            await dbContext.SaveChangesAsync();
            exampleEntity.State = EntityState.Detached;
        }
        catch (DbUpdateException)
        {
            //do nothing
        }
    }

    public async Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<long> taskExecutionIds,
        TasklingDbContext dbContext)
    {
        var taskExecutions = await dbContext.TaskExecutions.Where(i => taskExecutionIds.Contains(i.TaskExecutionId))
            .ToListAsync().ConfigureAwait(false);

        var results = new List<TaskExecutionState>();


        var currentDateTime = DateTime.UtcNow;
        foreach (var taskExecution in taskExecutions)
        {
            var teState = new TaskExecutionState
            {
                CompletedAt = taskExecution.CompletedAt,
                KeepAliveDeathThreshold = taskExecution.KeepAliveDeathThreshold,
                KeepAliveInterval = taskExecution.KeepAliveInterval,
                LastKeepAlive = taskExecution.LastKeepAlive,
                // reader.GetDateTimeEx("LastKeepAlive");
                OverrideThreshold = taskExecution.OverrideThreshold,
                //reader.GetTimeSpanEx("OverrideThreshold");
                StartedAt = taskExecution.StartedAt,
                // reader.GetDateTime("StartedAt");
                TaskDeathMode = (TaskDeathMode)taskExecution.TaskDeathMode,
                // reader.GetInt32("TaskDeathMode");
                TaskExecutionId = taskExecution.TaskExecutionId,
                //reader.GetInt32("TaskExecutionId");
                CurrentDateTime = currentDateTime
            };


            // reader.GetDateTime("CurrentDateTime");

            results.Add(teState);
        }

        return results;
    }

    public bool HasExpired(TaskExecutionState taskExecutionState)
    {
        return taskExecutionState.HasExpired();
    }
}