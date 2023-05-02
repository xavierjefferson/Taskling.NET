using TasklingTester.Common.Entities;

namespace TasklingTesterAsync.ListBlocks;

public interface INotificationService
{
    Task NotifyUserAsync(TravelInsight travelInsight);
}