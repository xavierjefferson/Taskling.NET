using TasklingTester.Common.Entities;

namespace TasklingTester.ListBlocks;

public interface INotificationService
{
    void NotifyUser(TravelInsight travelInsight);
}