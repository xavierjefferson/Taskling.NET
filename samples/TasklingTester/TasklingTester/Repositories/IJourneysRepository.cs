using TasklingTester.Entities;

namespace TasklingTester.Repositories
{
    public interface IJourneysRepository
    {
        long GetMaxJourneyId();
        DateTime GetMaxJourneyDate();
        IList<Journey> GetJourneys(long startId, long endId);
        IList<Journey> GetJourneys(DateTime startDate, DateTime endDate);
    }
}
