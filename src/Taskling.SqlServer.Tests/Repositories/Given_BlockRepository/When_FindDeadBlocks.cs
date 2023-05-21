using System;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.Tasks;
using Xunit;

namespace Taskling.SqlServer.Tests.Repositories.Given_BlockRepository;

[Collection(TestConstants.CollectionName)]
public class When_FindDeadBlocks : TestBase
{
    private readonly IBlockRepository _blockRepository;
    private readonly IBlocksHelper _blocksHelper;

    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_FindDeadBlocks> _logger;
    private readonly long _taskDefinitionId;
    private readonly TimeSpan FiveMinuteSpan = new(0, 5, 0);
    private readonly TimeSpan OneMinuteSpan = new(0, 1, 0);
    private readonly TimeSpan TwentySecondSpan = new(0, 0, 20);
    private long _block1;
    private long _block2;
    private long _block3;
    private long _block4;
    private long _block5;
    private long _taskExecution1;

    public When_FindDeadBlocks(IBlockRepository blockRepository, IBlocksHelper blocksHelper,
        IExecutionsHelper executionsHelper, ILogger<When_FindDeadBlocks> logger,
        ITaskRepository taskRepository) : base(executionsHelper)
    {
        _logger = logger;
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _blockRepository = blockRepository;
        _blocksHelper = blocksHelper;
        _blocksHelper.DeleteBlocks(CurrentTaskId.ApplicationName);
        _executionsHelper = executionsHelper;


        _executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);

        _taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
        _executionsHelper.InsertAvailableExecutionToken(_taskDefinitionId);

        taskRepository.ClearCache();
    }

    private void InsertDateRangeTestData(TaskDeathMode taskDeathMode)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var now = DateTime.UtcNow;
        if (taskDeathMode == TaskDeathMode.Override)
            _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, OneMinuteSpan,
                now.AddMinutes(-250), now.AddMinutes(-179));
        else
            _taskExecution1 = _executionsHelper.InsertKeepAliveTaskExecution(_taskDefinitionId, TwentySecondSpan,
                FiveMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));

        InsertDateRangeBlocksTestData();
    }

    private void InsertDateRangeBlocksTestData()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var now = DateTime.UtcNow;
        _block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-180), now.AddMinutes(-179));
        _block2 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-200), now.AddMinutes(-199));
        _block3 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-220), now.AddMinutes(-219));
        _block4 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-240), now.AddMinutes(-239));
        _block5 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-250), now.AddMinutes(-249));
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, now.AddMinutes(-180), now.AddMinutes(-180),
            now.AddMinutes(-175), BlockExecutionStatus.Failed, 2);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, now.AddMinutes(-200), now.AddMinutes(-200), null,
            BlockExecutionStatus.Started);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, now.AddMinutes(-220), null, null,
            BlockExecutionStatus.NotStarted);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, now.AddMinutes(-240), now.AddMinutes(-240),
            now.AddMinutes(-235), BlockExecutionStatus.Completed, 2);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block5, now.AddMinutes(-250), now.AddMinutes(-250), null,
            BlockExecutionStatus.Started, 3);
    }

    private void InsertNumericRangeTestData(TaskDeathMode taskDeathMode)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var now = DateTime.UtcNow;
        if (taskDeathMode == TaskDeathMode.Override)
            _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, OneMinuteSpan,
                now.AddMinutes(-250), now.AddMinutes(-179));
        else
            _taskExecution1 = _executionsHelper.InsertKeepAliveTaskExecution(_taskDefinitionId, TwentySecondSpan,
                FiveMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));

        InsertNumericRangeBlocksTestData();
    }

    private void InsertNumericRangeBlocksTestData()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var now = DateTime.UtcNow;
        _block1 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 1, 100, now.AddMinutes(-100));
        _block2 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 101, 200, now.AddMinutes(-90));
        _block3 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 201, 300, now.AddMinutes(-80));
        _block4 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 301, 400, now.AddMinutes(-70));
        _block5 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 401, 500, now.AddMinutes(-60));
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, now.AddMinutes(-180), now.AddMinutes(-180),
            now.AddMinutes(-175), BlockExecutionStatus.Failed, 2);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, now.AddMinutes(-200), now.AddMinutes(-200), null,
            BlockExecutionStatus.Started);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, now.AddMinutes(-220), null, null,
            BlockExecutionStatus.NotStarted);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, now.AddMinutes(-240), now.AddMinutes(-240),
            now.AddMinutes(-235), BlockExecutionStatus.Completed);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block5, now.AddMinutes(-250), now.AddMinutes(-250), null,
            BlockExecutionStatus.Started, 3);
    }

    private void InsertListTestData(TaskDeathMode taskDeathMode)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var now = DateTime.UtcNow;
        if (taskDeathMode == TaskDeathMode.Override)
            _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, OneMinuteSpan,
                now.AddMinutes(-250), now.AddMinutes(-179));
        else
            _taskExecution1 = _executionsHelper.InsertKeepAliveTaskExecution(_taskDefinitionId, TwentySecondSpan,
                FiveMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));

        InsertListBlocksTestData();
    }

    private void InsertListBlocksTestData()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var now = DateTime.UtcNow;
        _block1 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-246));
        _block2 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-247));
        _block3 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-248));
        _block4 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-249));
        _block5 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-250));
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, now.AddMinutes(-180), now.AddMinutes(-180),
            now.AddMinutes(-175), BlockExecutionStatus.Failed, 2);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, now.AddMinutes(-200), now.AddMinutes(-200), null,
            BlockExecutionStatus.Started);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, now.AddMinutes(-220), null, null,
            BlockExecutionStatus.NotStarted);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, now.AddMinutes(-240), now.AddMinutes(-240),
            now.AddMinutes(-235), BlockExecutionStatus.Completed);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block5, now.AddMinutes(-250), now.AddMinutes(-250), null,
            BlockExecutionStatus.Started, 3);
    }

    private void InsertObjectTestData(TaskDeathMode taskDeathMode)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var now = DateTime.UtcNow;
        if (taskDeathMode == TaskDeathMode.Override)
            _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, OneMinuteSpan,
                now.AddMinutes(-250), now.AddMinutes(-179));
        else
            _taskExecution1 = _executionsHelper.InsertKeepAliveTaskExecution(_taskDefinitionId, TwentySecondSpan,
                FiveMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));

        InsertObjectBlocksTestData();
    }

    private void InsertObjectBlocksTestData()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var now = DateTime.UtcNow;
        _block1 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-246), Guid.NewGuid().ToString());
        _block2 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-247), Guid.NewGuid().ToString());
        _block3 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-248), Guid.NewGuid().ToString());
        _block4 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-249), Guid.NewGuid().ToString());
        _block5 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-250), Guid.NewGuid().ToString());
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, now.AddMinutes(-180), now.AddMinutes(-180),
            now.AddMinutes(-175), BlockExecutionStatus.Failed, 2);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, now.AddMinutes(-200), now.AddMinutes(-200), null,
            BlockExecutionStatus.Started);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, now.AddMinutes(-220), null, null,
            BlockExecutionStatus.NotStarted);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, now.AddMinutes(-240), now.AddMinutes(-240),
            now.AddMinutes(-235), BlockExecutionStatus.Completed);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block5, now.AddMinutes(-250), now.AddMinutes(-250), null,
            BlockExecutionStatus.Started, 3);
    }

    private IBlockRepository CreateSut()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return _blockRepository;
    }

    private FindDeadBlocksRequest CreateDeadBlockRequest(BlockType blockType, TaskDeathMode taskDeathMode,
        int blockCountLimit)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return CreateDeadBlockRequest(blockType, taskDeathMode, blockCountLimit, 3, -300);
    }

    private FindDeadBlocksRequest CreateDeadBlockRequest(BlockType blockType, TaskDeathMode taskDeathMode,
        int blockCountLimit, int attemptLimit, int fromMinutesBack)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return new FindDeadBlocksRequest(CurrentTaskId,
            1,
            blockType,
            DateTime.UtcNow.AddMinutes(fromMinutesBack),
            DateTime.UtcNow,
            blockCountLimit,
            taskDeathMode,
            attemptLimit);
    }

    #region .: Date Range Blocks :.

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndDateRange_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.Override);
            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_OverrideModeAndDateRange_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.Override);
            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.Override, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndDateRange_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.Override);
            var blockCountLimit = 1;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(1, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndDateRange_SomeDeadTasksHaveReachedRetryLimit_ThenReturnOnlyDeadBlocksNotAtLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.Override);
            var blockCountLimit = 5;
            var retryLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.Override, blockCountLimit,
                retryLimit,
                -300);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_KeepAliveModeAndDateRange_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_KeepAliveModeAndDateRange_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_KeepAliveModeAndDateRange_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-2));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_KeepAliveModeAndDateRange_DeadTasksPassedKeepAliveLimitButAreOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            var blockCountLimit = 5;
            var request =
                CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.KeepAlive, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    #endregion .: Date Range Blocks :.

    #region .: Numeric Range Blocks :.

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndNumericRange_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.Override);
            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_OverrideModeAndNumericRange_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.Override);
            var blockCountLimit = 5;
            var request =
                CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.Override, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndNumericRange_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.Override);
            var blockCountLimit = 1;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(1, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndNumericRange_SomeDeadTasksHaveReachedRetryLimit_ThenReturnOnlyDeadBlocksNotAtLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.Override);
            var blockCountLimit = 5;
            var retryLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.Override, blockCountLimit,
                retryLimit, -300);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_KeepAliveModeAndNumericRange_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_KeepAliveModeAndNumericRange_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_KeepAliveModeAndNumericRange_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-2));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_KeepAliveModeAndNumericRange_DeadTasksPassedKeepAliveLimitButAreOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.KeepAlive, blockCountLimit, 3,
                -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    #endregion .: Date Range Blocks :.

    #region .: List Blocks :.

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndList_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.Override);
            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_OverrideModeAndList_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.Override);
            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.Override, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndList_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.Override);
            var blockCountLimit = 1;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(1, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_OverrideModeAndList_SomeDeadTasksHaveReachedRetryLimit_ThenReturnOnlyDeadBlocksNotAtLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.Override);
            var blockCountLimit = 5;
            var attemptLimit = 2;
            var request =
                CreateDeadBlockRequest(BlockType.List, TaskDeathMode.Override, blockCountLimit, attemptLimit, -300);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block3);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_KeepAliveModeAndList_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_KeepAliveModeAndList_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_KeepAliveModeAndList_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-2));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_KeepAliveModeAndList_DeadTasksPassedKeepAliveLimitButAreOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.KeepAlive, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    #endregion .: List Blocks :.

    #region .: Object Blocks :.

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndObject_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));

        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.Override);
            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_OverrideModeAndObject_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.Override);
            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.Override, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndObject_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.Override);
            var blockCountLimit = 1;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(1, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_OverrideModeAndObject_SomeDeadTasksHaveReachedRetryLimit_ThenReturnOnlyDeadBlocksNotAtLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.Override);
            var blockCountLimit = 5;
            var attemptLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.Override, blockCountLimit,
                attemptLimit,
                -300);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block3);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_KeepAliveModeAndObject_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_KeepAliveModeAndObject_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block5);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_KeepAliveModeAndObject_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-2));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_KeepAliveModeAndObject_DeadTasksPassedKeepAliveLimitButAreOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.KeepAlive, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    #endregion .: Object Blocks :.
}