using TasklingTester.Common.Entities;

namespace TasklingTesterAsync.Repositories;

public interface ITravelInsightsRepository
{
    Task AddAsync(IList<TravelInsight> insights);
}