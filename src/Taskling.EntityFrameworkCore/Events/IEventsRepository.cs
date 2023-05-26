using Taskling.Enums;
using Taskling.InfrastructureContracts;

namespace Taskling.EntityFrameworkCore.Events;

public interface IEventsRepository
{
    Task LogEventAsync(TaskId taskId, long taskExecutionId, EventTypeEnum eventType, string? message);
}