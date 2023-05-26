using Taskling;
using Taskling.Builders;
using Taskling.Contexts;
using Taskling.Enums;
using TasklingTester.Common.Entities;
using TasklingTesterAsync.Configuration;
using TasklingTesterAsync.Repositories;

namespace TasklingTesterAsync.DateRangeBlocks;

public class TravelInsightsAnalysisService
{
    private readonly IMyApplicationConfiguration _configuration;
    private readonly ITasklingClient _tasklingClient;
    private readonly IJourneysRepository _travelDataService;
    private readonly ITravelInsightsRepository _travelInsightsService;

    public TravelInsightsAnalysisService(ITasklingClient tasklingClient,
        IMyApplicationConfiguration configuration,
        IJourneysRepository myTravelDataService,
        ITravelInsightsRepository travelInsightsService)
    {
        _tasklingClient = tasklingClient;
        _configuration = configuration;
        _travelDataService = myTravelDataService;
        _travelInsightsService = travelInsightsService;
    }

    public async Task RunBatchJobAsync()
    {
        var builder = new DateRangeJobBuilder().WithClient(_tasklingClient).WithApplication("MyApplication")
            .WithTaskName("MyDateBasedBatchJob")
            .WithRange((Func<ITaskExecutionContext, Task<DateRange>>)(async taskExecutionContext =>
            {
                DateTime startDate;
                var lastBlock = await taskExecutionContext.GetLastDateRangeBlockAsync(LastBlockOrderEnum.LastCreated);
                if (lastBlock == null)
                    startDate = _configuration.FirstRunDate;
                else startDate = lastBlock.EndDate;
                var endDate = DateTime.Now;
                return new DateRange(startDate, endDate, TimeSpan.FromMinutes(30));
            })).WithProcessFunc((Func<IDateRangeBlockContext, Task>)ProcessBlockAsync);

        await builder.Build().Execute();
    }

    private async Task ProcessBlockAsync(IDateRangeBlockContext blockContext)
    {
        try
        {
            await blockContext.StartAsync();

            var journeys = await _travelDataService.GetJourneysAsync(blockContext.DateRangeBlock.StartDate,
                blockContext.DateRangeBlock.EndDate);
            var travelInsights = new List<TravelInsight>();

            foreach (var journey in journeys)
            {
                var insight = new TravelInsight
                {
                    InsightDate = journey.TravelDate.Date,
                    InsightText = "Some useful insight",
                    PassengerName = journey.PassengerName
                };

                travelInsights.Add(insight);
            }

            await _travelInsightsService.AddAsync(travelInsights);

            var itemCountProcessed = travelInsights.Count;
            await blockContext.CompleteAsync(itemCountProcessed);
        }
        catch (Exception ex)
        {
            await blockContext.FailedAsync(ex.ToString());
        }
    }
}