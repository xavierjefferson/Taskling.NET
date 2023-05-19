using Microsoft.EntityFrameworkCore;
using Taskling.SqlServer.Models;
using Taskling.Tasks;

namespace Taskling.SqlServer.Tokens;

public class CommonTokenRepository : ICommonTokenRepository
{
    public async Task AcquireRowLockAsync(int taskDefinitionId, int taskExecutionId,
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
        //exampleEntity.ExampleProperty = "abc";
        //dbcontext.Entry<TaskDefinition>(exampleEntity).Property(ee => ee.ExampleProperty).IsModified = true;
        //dbcontext.Configuration.ValidateOnSaveEnabled = false;
        //dbcontext.SaveChanges();


        //var taskDefinitions =
        //    await dbContext.TaskDefinitions.Where(i => i.TaskDefinitionId == taskDefinitionId).ToListAsync();
        //foreach (var taskDefinition in taskDefinitions)
        //{
        //    taskDefinition.HoldLockTaskExecutionId = taskExecutionId;
        //    dbContext.TaskDefinitions.Update(taskDefinition);
        //}

        //await dbContext.SaveChangesAsync();
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
        return taskExecutionState.HasExpired();
    }
}