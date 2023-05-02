using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TasklingTester.Common.Entities;

namespace TasklingTesterAsync.Repositories
{
    public interface ITravelInsightsRepository
    {
        Task AddAsync(IList<TravelInsight> insights);
    }
}
