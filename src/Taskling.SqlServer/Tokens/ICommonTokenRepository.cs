using System.Data.SqlClient;
using Taskling.Models123;

namespace Taskling.SqlServer.Tokens;

public interface ICommonTokenRepository
{
    Task AcquireRowLockAsync(int taskDefinitionId, int taskExecutionId, TasklingDbContext dbContext);
    Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<int> taskExecutionIds,
        TasklingDbContext dbContext);
    bool HasExpired(TaskExecutionState taskExecutionState);
}