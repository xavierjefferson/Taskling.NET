using System.Data;
using System.Data.SqlClient;
using Taskling.Events;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.AncilliaryServices;

namespace Taskling.SqlServer.Events;

public class EventsRepository : DbOperationsService, IEventsRepository
{
    public async Task LogEventAsync(TaskId taskId, int taskExecutionId, EventType eventType, string message)
    {
        using (var context = await GetDbContextAsync(taskId).ConfigureAwait(false))
        {
            var taskExecutionEvent = new Models123.TaskExecutionEvent()
            {
                TaskExecutionId = taskExecutionId,
                EventType = (int)eventType,
                Message = message,
                EventDateTime = DateTime.UtcNow
            };
            context.TaskExecutionEvents.Add(taskExecutionEvent);
            await context.SaveChangesAsync();
        }
    }
}