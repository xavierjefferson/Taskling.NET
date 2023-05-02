using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.SqlServer;
using TasklingTester.Configuration;
using TasklingTester.Repositories;
using TasklingTester.ListBlocks;
using Microsoft.Extensions.DependencyInjection;
using Taskling.Configuration;

namespace TasklingTester
{
    class Program
    {
        static void Main(string[] args)
        {   
            //setup our DI
            var serviceCollection = new ServiceCollection()
                .AddLogging()
                .AddTaskling();
            serviceCollection.AddSingleton<IConfigurationReader, TasklingIConfigurationReader>();
            var serviceProvider = serviceCollection
                .BuildServiceProvider();
            //var insightService = GetDateRangeInsightService();
            //insightService.RunBatchJob();

            //var insightService = GetNumericRangeInsightService();
            //insightService.RunBatchJob();

            var insightService = GetListInsightService(serviceProvider);
            insightService.RunBatchJob();
        }

        private static DateRangeBlocks.TravelInsightsAnalysisService GetDateRangeInsightService(IServiceProvider serviceProvider)
        {
            var my = serviceProvider.GetRequiredService<IConfigurationReader>();
            return new DateRangeBlocks.TravelInsightsAnalysisService(new TasklingClient(serviceProvider, my),
                new MyApplicationConfiguration(),
                new JourneysRepository(),
                new TravelInsightsRepository());

        }

        private static NumericRangeBlocks.TravelInsightsAnalysisService GetNumericRangeInsightService(IServiceProvider serviceProvider)
        {
            var my = serviceProvider.GetRequiredService<IConfigurationReader>();
            return new NumericRangeBlocks.TravelInsightsAnalysisService(new TasklingClient(serviceProvider, my),
                new MyApplicationConfiguration(),
                new JourneysRepository(),
                new TravelInsightsRepository());

        }

        private static ListBlocks.TravelInsightsAnalysisService GetListInsightService(IServiceProvider serviceProvider)
        {
            var my = serviceProvider.GetRequiredService<IConfigurationReader>();
            return new ListBlocks.TravelInsightsAnalysisService(new TasklingClient(serviceProvider, my),
                new MyApplicationConfiguration(),
                new JourneysRepository(),
                new NotificationService());

        }
    }
}
