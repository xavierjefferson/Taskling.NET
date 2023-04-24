using System.Data;
using System.Data.SqlClient;
using Taskling.Tasks;

namespace Taskling.SqlServer.Tokens;

public class CommonTokenRepository : ICommonTokenRepository
{
    public async Task AcquireRowLockAsync(int taskDefinitionId, int taskExecutionId, SqlCommand command)
    {
        command.Parameters.Clear();
        command.CommandText = TokensQueryBuilder.AcquireLockQuery;
        command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
        command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    public async Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<int> taskExecutionIds,
        SqlCommand command)
    {
        var results = new List<TaskExecutionState>();

        command.Parameters.Clear();
        command.CommandText = TokensQueryBuilder.GetTaskExecutions(taskExecutionIds.Count);

        for (var i = 0; i < taskExecutionIds.Count; i++)
            command.Parameters.Add("@InParam" + i, SqlDbType.Int).Value = taskExecutionIds[i];

        using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
        {
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var teState = new TaskExecutionState();


                teState.CompletedAt = reader.GetDateTimeEx("CompletedAt");


                teState.KeepAliveDeathThreshold = reader.GetTimeSpanEx("KeepAliveDeathThreshold");


                teState.KeepAliveInterval = reader.GetTimeSpanEx("KeepAliveInterval");


                teState.LastKeepAlive = reader.GetDateTimeEx("LastKeepAlive");


                teState.OverrideThreshold = reader.GetTimeSpanEx("OverrideThreshold");

                teState.StartedAt = reader.GetDateTime("StartedAt");
                teState.TaskDeathMode = (TaskDeathMode)reader.GetInt32("TaskDeathMode");
                teState.TaskExecutionId = reader.GetInt32("TaskExecutionId");
                teState.CurrentDateTime = reader.GetDateTime("CurrentDateTime");

                results.Add(teState);
            }
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