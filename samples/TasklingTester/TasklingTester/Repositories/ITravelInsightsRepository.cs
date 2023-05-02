using TasklingTester.Common.Entities;

namespace TasklingTester.Repositories;

public interface ITravelInsightsRepository
{
    void Add(IList<TravelInsight> insights);
}