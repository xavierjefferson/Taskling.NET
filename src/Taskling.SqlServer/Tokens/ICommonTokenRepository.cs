using Taskling.SqlServer.Models;

namespace Taskling.SqlServer.Tokens;

public interface ICommonTokenRepository
{
    Task AcquireRowLockAsync(int taskDefinitionId, int taskExecutionId, TasklingDbContext dbContext);

    Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<int> taskExecutionIds,
        TasklingDbContext dbContext);

    bool HasExpired(TaskExecutionState taskExecutionState);
}