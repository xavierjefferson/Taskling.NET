using Taskling.SqlServer.Models;

namespace Taskling.SqlServer.Tokens;

public interface ICommonTokenRepository
{
    Task AcquireRowLockAsync(long taskDefinitionId, long taskExecutionId, TasklingDbContext dbContext);

    Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<long> taskExecutionIds,
        TasklingDbContext dbContext);

    bool HasExpired(TaskExecutionState taskExecutionState);
}