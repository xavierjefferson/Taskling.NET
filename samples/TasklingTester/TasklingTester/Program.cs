using Microsoft.Extensions.DependencyInjection;
using Taskling.Configuration;
using Taskling.SqlServer;
using TasklingTester.Configuration;
using TasklingTester.ListBlocks;
using TasklingTester.Repositories;
using TravelInsightsAnalysisService = TasklingTester.DateRangeBlocks.TravelInsightsAnalysisService;

namespace TasklingTester;

internal class Program
{
    private static void Main(string[] args)
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

    private static TravelInsightsAnalysisService GetDateRangeInsightService(IServiceProvider serviceProvider)
    {
        var my = serviceProvider.GetRequiredService<IConfigurationReader>();
        return new TravelInsightsAnalysisService(new TasklingClient(serviceProvider, my),
            new MyApplicationConfiguration(),
            new JourneysRepository(),
            new TravelInsightsRepository());
    }

    private static NumericRangeBlocks.TravelInsightsAnalysisService GetNumericRangeInsightService(
        IServiceProvider serviceProvider)
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