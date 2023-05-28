using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Taskling.Configuration;
using Taskling.EntityFrameworkCore;
using Taskling.EntityFrameworkCore.Extensions;
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
            .AddTaskling(builder =>
            {
                builder.WithReader<MyConfigReader>().WithDbContextOptions(i => i.Builder.UseSqlServer(i.ConnectionString));
            });
     
        serviceCollection.AddSingleton<ITaskConfigurationReader, MyConfigReader>();
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
        var my = serviceProvider.GetRequiredService<ITaskConfigurationReader>();
        return new TravelInsightsAnalysisService(new TasklingClient(serviceProvider, my),
            new MyApplicationConfiguration(),
            new JourneysRepository(),
            new TravelInsightsRepository());
    }

    private static NumericRangeBlocks.TravelInsightsAnalysisService GetNumericRangeInsightService(
        IServiceProvider serviceProvider)
    {
        var my = serviceProvider.GetRequiredService<ITaskConfigurationReader>();
        return new NumericRangeBlocks.TravelInsightsAnalysisService(new TasklingClient(serviceProvider, my),
            new MyApplicationConfiguration(),
            new JourneysRepository(),
            new TravelInsightsRepository());
    }

    private static ListBlocks.TravelInsightsAnalysisService GetListInsightService(IServiceProvider serviceProvider)
    {
        var my = serviceProvider.GetRequiredService<ITaskConfigurationReader>();

        return new ListBlocks.TravelInsightsAnalysisService(new TasklingClient(serviceProvider, my),
            new MyApplicationConfiguration(),
            new JourneysRepository(),
            new NotificationService());
    }
}