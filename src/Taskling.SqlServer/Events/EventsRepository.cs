using Microsoft.Extensions.Logging;
using Taskling.Events;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Models;
using TransactionScopeRetryHelper;

namespace Taskling.SqlServer.Events;

public class EventsRepository : DbOperationsService, IEventsRepository
{
    public EventsRepository(IConnectionStore connectionStore, IDbContextFactoryEx dbContextFactoryEx,
        ILoggerFactory loggerFactory) : base(
        connectionStore, dbContextFactoryEx, loggerFactory.CreateLogger<DbOperationsService>())
    {
    }

    public async Task LogEventAsync(TaskId taskId, long taskExecutionId, EventType eventType, string? message)
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