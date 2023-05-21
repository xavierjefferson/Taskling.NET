using Taskling;
using Taskling.Blocks.ListBlocks;
using Taskling.Contexts;
using TasklingTester.Common.Entities;
using TasklingTester.Common.ListBlocks;
using TasklingTester.Configuration;
using TasklingTester.Repositories;

namespace TasklingTester.ListBlocks;

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

    public void RunBatchJob()
    {
        using (var taskExecutionContext =
               _tasklingClient.CreateTaskExecutionContext("MyApplication", "MyListBasedBatchJob"))
        {
            if (taskExecutionContext.TryStart()) RunTask(taskExecutionContext);
        }
    }

    private void RunTask(ITaskExecutionContext taskExecutionContext)
    {
        try
        {
            var listBlocks = GetListBlocks(taskExecutionContext);
            foreach (var block in listBlocks)
                ProcessBlock(block);

            taskExecutionContext.Complete();
        }
        catch (Exception ex)
        {
            taskExecutionContext.Error(ex.ToString(), true);
        }
    }

    private IList<IListBlockContext<Journey, BatchDatesHeader>> GetListBlocks(
        ITaskExecutionContext taskExecutionContext)
    {
        using (var cs = taskExecutionContext.CreateCriticalSection())
        {
            if (cs.TryStart())
            {
                var startDate = GetDateRangeStartDate(taskExecutionContext);
                var endDate = DateTime.Now;

                var journeys = _travelDataService.GetJourneys(startDate, endDate).ToList();
                var batchHeader = new BatchDatesHeader
                {
                    FromDate = startDate,
                    ToDate = endDate
                };

                var blockSize = 500;
                return taskExecutionContext.GetListBlocks<Journey, BatchDatesHeader>(x =>
                    x.WithPeriodicCommit(journeys, batchHeader, blockSize, BatchSize.Fifty));
            }

            throw new Exception("Could not acquire a critical section, aborted task");
        }
    }

    private DateTime GetDateRangeStartDate(ITaskExecutionContext taskExecutionContext)
    {
        var lastBlock = taskExecutionContext.GetLastListBlock<Journey, BatchDatesHeader>();
        if (lastBlock == null)
            return _configuration.FirstRunDate;
        return lastBlock.Header.ToDate;
    }

    private void ProcessBlock(IListBlockContext<Journey, BatchDatesHeader> blockContext)
    {
        try
        {
            blockContext.Start();

            foreach (var journeyItem in blockContext.GetItems(ItemStatus.Pending, ItemStatus.Failed))
                ProcessJourney(journeyItem);

            blockContext.Complete();
        }
        catch (Exception ex)
        {
            blockContext.Failed(ex.ToString());
        }
    }

    private void ProcessJourney(IListBlockItem<Journey> journeyItem)
    {
        try
        {
            if (journeyItem.Value.DepartureStation.Equals(journeyItem.Value.ArrivalStation))
            {
                journeyItem.Discarded("Discarded due to distance rule");
            }
            else
            {
                var insight = ExtractInsight(journeyItem.Value);
                _notificationService.NotifyUser(insight);
                journeyItem.Complete();
            }
        }
        catch (Exception ex)
        {
            journeyItem.Failed(ex.ToString());
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