using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.AncilliaryServices;
using Taskling.EntityFrameworkCore.Models;
using Taskling.Enums;
using Taskling.InfrastructureContracts;

namespace Taskling.EntityFrameworkCore.Events;

public class EventsRepository : DbOperationsService, IEventsRepository
{
    public EventsRepository( IDbContextFactoryEx dbContextFactoryEx,
        ILoggerFactory loggerFactory) : base(dbContextFactoryEx, loggerFactory.CreateLogger<DbOperationsService>())
    {
    }

    public async Task LogEventAsync(TaskId taskId, long taskExecutionId, EventTypeEnum eventType, string? message)
    {
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var context = await GetDbContextAsync(taskId).ConfigureAwait(false))
            {
                var taskExecutionEvent = new TaskExecutionEvent
                {
                    TaskExecutionId = taskExecutionId,
                    EventType = (int)eventType,
                    Message = message,
                    EventDateTime = DateTime.UtcNow
                };
                context.TaskExecutionEvents.Add(taskExecutionEvent);
                await context.SaveChangesAsync();
            }
        });
    }
}