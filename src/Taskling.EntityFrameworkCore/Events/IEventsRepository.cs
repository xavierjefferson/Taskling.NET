using Taskling.Events;
using Taskling.InfrastructureContracts;

namespace Taskling.SqlServer.Events;

public interface IEventsRepository
{
    Task LogEventAsync(TaskId taskId, long taskExecutionId, EventType eventType, string? message);
}