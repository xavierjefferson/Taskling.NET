using Taskling;
using Taskling.Blocks.ListBlocks;
using Taskling.Contexts;
using Taskling.InfrastructureContracts;
using TasklingTester.Common.Entities;
using TasklingTester.Common.ListBlocks;
using TasklingTesterAsync.Configuration;
using TasklingTesterAsync.Repositories;

namespace TasklingTesterAsync.ListBlocks;

public class TravelInsightsAnalysisService
{
    private readonly IMyApplicationConfiguration _configuration;
    private readonly INotificationService _notificationService;
    private readonly ITasklingClient _tasklingClient;
    private readonly IJourneysRepository _travelDataService;

    public TravelInsightsAnalysisService(ITasklingClient tasklingClient,
        IMyApplicationConfiguration configuration,
        IJourneysRepository myTravelDataService,
        INotificationService notificationService)
    {
        _tasklingClient = tasklingClient;
        _configuration = configuration;
        _travelDataService = myTravelDataService;
        _notificationService = notificationService;
    }

    public async Task RunBatchJobAsync()
    {
        using (var taskExecutionContext =
               _tasklingClient.CreateTaskExecutionContext("MyApplication", "MyListBasedBatchJob"))
        {
            if (await taskExecutionContext.TryStartAsync()) await RunTaskAsync(taskExecutionContext);
        }
    }

    private async Task RunTaskAsync(ITaskExecutionContext taskExecutionContext)
    {
        try
        {
            var listBlocks = await GetListBlocksAsync(taskExecutionContext);
            foreach (var block in listBlocks)
                await ProcessBlockAsync(block);

            await taskExecutionContext.CompleteAsync();
        }
        catch (Exception ex)
        {
            await taskExecutionContext.ErrorAsync(ex.ToString(), true);
        }
    }

    private async Task<IList<IListBlockContext<Journey, BatchDatesHeader>>> GetListBlocksAsync(
        ITaskExecutionContext taskExecutionContext)
    {
        using (var cs = taskExecutionContext.CreateCriticalSection())
        {
            if (await cs.TryStartAsync())
            {
                var startDate = await GetDateRangeStartDateAsync(taskExecutionContext);
                var endDate = DateTime.Now;

                var journeys = await _travelDataService.GetJourneysAsync(startDate, endDate);
                var batchHeader = new BatchDatesHeader
                {
                    FromDate = startDate,
                    ToDate = endDate
                };

                short blockSize = 500;
                return await taskExecutionContext.GetListBlocksAsync<Journey, BatchDatesHeader>(x =>
                    x.WithPeriodicCommit(journeys.ToList(), batchHeader, blockSize, BatchSize.Fifty));
            }

            throw new Exception("Could not acquire a critical section, aborted task");
        }
    }

    private async Task<DateTime> GetDateRangeStartDateAsync(ITaskExecutionContext taskExecutionContext)
    {
        var lastBlock = await taskExecutionContext.GetLastListBlockAsync<Journey, BatchDatesHeader>();
        if (lastBlock == null)
            return _configuration.FirstRunDate;
        return lastBlock.Header.ToDate;
    }

    private async Task ProcessBlockAsync(IListBlockContext<Journey, BatchDatesHeader> blockContext)
    {
        try
        {
            await blockContext.StartAsync();

            foreach (var journeyItem in await blockContext.GetItemsAsync(ItemStatus.Pending, ItemStatus.Failed))
                await ProcessJourneyAsync(journeyItem);

            await blockContext.CompleteAsync();
        }
        catch (Exception ex)
        {
            await blockContext.FailedAsync(ex.ToString());
        }
    }

    private async Task ProcessJourneyAsync(IListBlockItem<Journey> journeyItem)
    {
        try
        {
            if (journeyItem.Value.DepartureStation.Equals(journeyItem.Value.ArrivalStation))
            {
                await journeyItem.DiscardedAsync("Discarded due to distance rule");
            }
            else
            {
                var insight = ExtractInsight(journeyItem.Value);
                await _notificationService.NotifyUserAsync(insight);
                await journeyItem.CompleteAsync();
            }
        }
        catch (Exception ex)
        {
            await journeyItem.FailedAsync(ex.ToString());
        }
    }

    private TravelInsight ExtractInsight(Journey journey)
    {
        var insight = new TravelInsight
        {
            InsightDate = journey.TravelDate.Date,
            InsightText = "Some useful insight",
            PassengerName = journey.PassengerName
        };

        return insight;
    }
}