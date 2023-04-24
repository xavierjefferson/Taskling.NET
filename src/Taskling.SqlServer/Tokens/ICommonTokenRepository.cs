using System.Data.SqlClient;

namespace Taskling.SqlServer.Tokens;

public interface ICommonTokenRepository
{
    Task AcquireRowLockAsync(int taskDefinitionId, int taskExecutionId, SqlCommand command);
    Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<int> taskExecutionIds, SqlCommand command);
    bool HasExpired(TaskExecutionState taskExecutionState);
}