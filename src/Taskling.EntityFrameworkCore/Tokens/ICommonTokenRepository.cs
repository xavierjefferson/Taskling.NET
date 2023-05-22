using Taskling.EntityFrameworkCore.Models;

namespace Taskling.EntityFrameworkCore.Tokens;

public interface ICommonTokenRepository
{
    Task AcquireRowLockAsync(long taskDefinitionId, long taskExecutionId, TasklingDbContext dbContext);

    Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<long> taskExecutionIds,
        TasklingDbContext dbContext);

    bool HasExpired(TaskExecutionState taskExecutionState);
}