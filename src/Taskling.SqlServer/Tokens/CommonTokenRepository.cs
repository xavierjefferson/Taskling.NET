using Microsoft.EntityFrameworkCore;
using Taskling.SqlServer.Models;
using Taskling.Tasks;

namespace Taskling.SqlServer.Tokens;

public class CommonTokenRepository : ICommonTokenRepository
{
    public async Task AcquireRowLockAsync(int taskDefinitionId, int taskExecutionId,
        TasklingDbContext dbContext)
    {
        var taskDefinitions =
            await dbContext.TaskDefinitions.Where(i => i.TaskDefinitionId == taskDefinitionId).ToListAsync();
        foreach (var taskDefinition in taskDefinitions)
        {
            taskDefinition.HoldLockTaskExecutionId = taskExecutionId;
            dbContext.TaskDefinitions.Update(taskDefinition);
        }

        await dbContext.SaveChangesAsync();
    }

    public async Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<int> taskExecutionIds,
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
        if (taskExecutionState.CompletedAt.HasValue)
            return true;

        if (taskExecutionState.TaskDeathMode == TaskDeathMode.KeepAlive)
        {
            if (!taskExecutionState.LastKeepAlive.HasValue)
                return true;

            var lastKeepAliveDiff = taskExecutionState.CurrentDateTime - taskExecutionState.LastKeepAlive.Value;
            if (lastKeepAliveDiff > taskExecutionState.KeepAliveDeathThreshold)
                return true;

            return false;
        }

        var activePeriod = taskExecutionState.CurrentDateTime - taskExecutionState.StartedAt;
        if (activePeriod > taskExecutionState.OverrideThreshold)
            return true;

        return false;
    }
}