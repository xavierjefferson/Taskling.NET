using TasklingTester.Entities;

namespace TasklingTester.Repositories
{
    public interface ITravelInsightsRepository
    {
        void Add(IList<TravelInsight> insights);
    }
}
