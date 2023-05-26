using Taskling;
using Taskling.Builders;
using Taskling.Contexts;
using Taskling.Enums;
using TasklingTester.Common.Entities;
using TasklingTesterAsync.Configuration;
using TasklingTesterAsync.Repositories;

namespace TasklingTesterAsync.NumericRangeBlocks;

public class TravelInsightsAnalysisService
{
    private readonly ITasklingClient _tasklingClient;
    private readonly IJourneysRepository _travelDataService;
    private readonly ITravelInsightsRepository _travelInsightsService;
    private IMyApplicationConfiguration _configuration;

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
        var builder = new NumericRangeJobBuilder().WithClient(_tasklingClient).WithApplication("MyApplication")
            .WithTaskName("MyNumericBasedBatchJob")
            .WithRange(async taskExecutionContext =>
            {
                long startNumber;
                var lastBlock =
                    await taskExecutionContext.GetLastNumericRangeBlockAsync(LastBlockOrderEnum.LastCreated);
                var maxJourneyId = await _travelDataService.GetMaxJourneyIdAsync();

                // if this is the first run then just process the last 1000
                if (lastBlock == null)
                    startNumber = maxJourneyId - 1000;
                // if there is no new data then just return any old blocks that have failed or died
                else if (lastBlock.EndNumber == maxJourneyId)
                    return await taskExecutionContext.GetNumericRangeBlocksAsync(x => x.WithOnlyOldNumericBlocks());
                // startNumber is the next unprocessed id
                else
                    startNumber = lastBlock.EndNumber + 1;

                var maxBlockSize = 500;
                return await taskExecutionContext.GetNumericRangeBlocksAsync(x =>
                    x.WithRange(startNumber, maxJourneyId, maxBlockSize));
            });
        builder.WithProcessFunc(ProcessBlockAsync);

        await builder.Build().Execute();
    }

    private async Task ProcessBlockAsync(INumericRangeBlockContext blockContext)
    {
        try
        {
            await blockContext.StartAsync();

            var journeys = await _travelDataService.GetJourneysAsync(blockContext.NumericRangeBlock.StartNumber,
                blockContext.NumericRangeBlock.EndNumber);
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