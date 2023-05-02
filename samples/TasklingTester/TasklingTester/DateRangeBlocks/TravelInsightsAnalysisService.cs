using Taskling;
using Taskling.Blocks.Common;
using Taskling.Contexts;
using TasklingTester.Common.Entities;
using TasklingTester.Configuration;
using TasklingTester.Repositories;

namespace TasklingTester.DateRangeBlocks;

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

    public void RunBatchJob()
    {
        using (var taskExecutionContext =
               _tasklingClient.CreateTaskExecutionContext("MyApplication", "MyDateBasedBatchJob"))
        {
            if (taskExecutionContext.TryStart()) RunTask(taskExecutionContext);
        }
    }

    private void RunTask(ITaskExecutionContext taskExecutionContext)
    {
        try
        {
            var dateRangeBlocks = GetDateRangeBlocks(taskExecutionContext);
            foreach (var block in dateRangeBlocks)
                ProcessBlock(block);

            taskExecutionContext.Complete();
        }
        catch (Exception ex)
        {
            taskExecutionContext.Error(ex.ToString(), true);
        }
    }

    private IList<IDateRangeBlockContext> GetDateRangeBlocks(ITaskExecutionContext taskExecutionContext)
    {
        using (var cs = taskExecutionContext.CreateCriticalSection())
        {
            if (cs.TryStart())
            {
                var startDate = GetDateRangeStartDate(taskExecutionContext);
                var endDate = DateTime.Now;

                return taskExecutionContext.GetDateRangeBlocks(x =>
                    x.WithRange(startDate, endDate, TimeSpan.FromMinutes(30)));
            }

            throw new Exception("Could not acquire a critical section, aborted task");
        }
    }

    private DateTime GetDateRangeStartDate(ITaskExecutionContext taskExecutionContext)
    {
        var lastBlock = taskExecutionContext.GetLastDateRangeBlock(LastBlockOrder.LastCreated);
        if (lastBlock == null)
            return _configuration.FirstRunDate;
        return lastBlock.EndDate;
    }

    private void ProcessBlock(IDateRangeBlockContext blockContext)
    {
        try
        {
            blockContext.Start();

            var journeys = _travelDataService.GetJourneys(blockContext.DateRangeBlock.StartDate,
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

            _travelInsightsService.Add(travelInsights);

            var itemCountProcessed = travelInsights.Count;
            blockContext.Complete(itemCountProcessed);
        }
        catch (Exception ex)
        {
            blockContext.Failed(ex.ToString());
        }
    }
}