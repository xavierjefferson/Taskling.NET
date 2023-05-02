using Microsoft.Extensions.DependencyInjection;
using Taskling.Configuration;
using Taskling.SqlServer;
using TasklingTesterAsync.Configuration;
using TasklingTesterAsync.ListBlocks;
using TasklingTesterAsync.Repositories;
using TravelInsightsAnalysisService = TasklingTesterAsync.DateRangeBlocks.TravelInsightsAnalysisService;

namespace TasklingTesterAsync;

internal class Program
{
    private static async Task Main(string[] args)
    {
        //setup our DI
        var serviceCollection = new ServiceCollection()
            .AddLogging()
            .AddTaskling();
        serviceCollection.AddSingleton<IConfigurationReader, MyConfigReader>();
        var serviceProvider = serviceCollection
            .BuildServiceProvider();


        //var insightService = GetDateRangeInsightService(serviceProvider);
        //await insightService.RunBatchJobAsync();

        //var insightService = GetNumericRangeInsightService(serviceProvider);
        //await insightService.RunBatchJobAsync();

        var insightService = GetListInsightService(serviceProvider);
        await insightService.RunBatchJobAsync();
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