using TasklingTester.Common.Entities;

namespace TasklingTesterAsync.ListBlocks;

public class NotificationService : INotificationService
{
    public async Task NotifyUserAsync(TravelInsight travelInsight)
    {
        // send a push notification or something
        await Task.Yield();
    }
}