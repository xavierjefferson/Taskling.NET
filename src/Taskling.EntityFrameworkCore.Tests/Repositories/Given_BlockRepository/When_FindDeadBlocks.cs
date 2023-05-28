using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.Enums;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Repositories.Given_BlockRepository;

[Collection(CollectionName)]
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
        _blockRepository = blockRepository;
        _blocksHelper = blocksHelper;
        _blocksHelper.DeleteBlocks(CurrentTaskId);
        _executionsHelper = executionsHelper;
        _executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);

        _taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
        _executionsHelper.InsertAvailableExecutionToken(_taskDefinitionId);

        taskRepository.ClearCache();
    }

    private void InsertDateRangeTestData(TaskDeathModeEnum taskDeathMode)
    {
        var now = DateTime.UtcNow;
        if (taskDeathMode == TaskDeathModeEnum.Override)
            _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, OneMinuteSpan,
                now.AddMinutes(-250), now.AddMinutes(-179));
        else
            _taskExecution1 = _executionsHelper.InsertKeepAliveTaskExecution(_taskDefinitionId, TwentySecondSpan,
                FiveMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));

        InsertDateRangeBlocksTestData();
    }

    private void InsertDateRangeBlocksTestData()
    {
        var now = DateTime.UtcNow;
        _block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-180), now.AddMinutes(-179), CurrentTaskId);
        _block2 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-200), now.AddMinutes(-199), CurrentTaskId);
        _block3 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-220), now.AddMinutes(-219), CurrentTaskId);
        _block4 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-240), now.AddMinutes(-239), CurrentTaskId);
        _block5 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-250), now.AddMinutes(-249), CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, now.AddMinutes(-180), now.AddMinutes(-180),
            now.AddMinutes(-175), BlockExecutionStatusEnum.Failed, CurrentTaskId, 2);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, now.AddMinutes(-200), now.AddMinutes(-200), null,
            BlockExecutionStatusEnum.Started, CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, now.AddMinutes(-220), null, null,
            BlockExecutionStatusEnum.NotStarted, CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, now.AddMinutes(-240), now.AddMinutes(-240),
            now.AddMinutes(-235), BlockExecutionStatusEnum.Completed, CurrentTaskId, 2);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block5, now.AddMinutes(-250), now.AddMinutes(-250), null,
            BlockExecutionStatusEnum.Started, CurrentTaskId, 3);
    }

    private void InsertNumericRangeTestData(TaskDeathModeEnum taskDeathMode)
    {
        var now = DateTime.UtcNow;
        if (taskDeathMode == TaskDeathModeEnum.Override)
            _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, OneMinuteSpan,
                now.AddMinutes(-250), now.AddMinutes(-179));
        else
            _taskExecution1 = _executionsHelper.InsertKeepAliveTaskExecution(_taskDefinitionId, TwentySecondSpan,
                FiveMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));

        InsertNumericRangeBlocksTestData();
    }

    private void InsertNumericRangeBlocksTestData()
    {
        var now = DateTime.UtcNow;
        _block1 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 1, 100, now.AddMinutes(-100), CurrentTaskId);
        _block2 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 101, 200, now.AddMinutes(-90), CurrentTaskId);
        _block3 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 201, 300, now.AddMinutes(-80), CurrentTaskId);
        _block4 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 301, 400, now.AddMinutes(-70), CurrentTaskId);
        _block5 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 401, 500, now.AddMinutes(-60), CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, now.AddMinutes(-180), now.AddMinutes(-180),
            now.AddMinutes(-175), BlockExecutionStatusEnum.Failed, CurrentTaskId, 2);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, now.AddMinutes(-200), now.AddMinutes(-200), null,
            BlockExecutionStatusEnum.Started, CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, now.AddMinutes(-220), null, null,
            BlockExecutionStatusEnum.NotStarted, CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, now.AddMinutes(-240), now.AddMinutes(-240),
            now.AddMinutes(-235), BlockExecutionStatusEnum.Completed, CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block5, now.AddMinutes(-250), now.AddMinutes(-250), null,
            BlockExecutionStatusEnum.Started, CurrentTaskId, 3);
    }

    private void InsertListTestData(TaskDeathModeEnum taskDeathMode)
    {
        var now = DateTime.UtcNow;
        if (taskDeathMode == TaskDeathModeEnum.Override)
            _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, OneMinuteSpan,
                now.AddMinutes(-250), now.AddMinutes(-179));
        else
            _taskExecution1 = _executionsHelper.InsertKeepAliveTaskExecution(_taskDefinitionId, TwentySecondSpan,
                FiveMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));

        InsertListBlocksTestData();
    }

    private void InsertListBlocksTestData()
    {
        var now = DateTime.UtcNow;
        _block1 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-246), CurrentTaskId);
        _block2 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-247), CurrentTaskId);
        _block3 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-248), CurrentTaskId);
        _block4 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-249), CurrentTaskId);
        _block5 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-250), CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, now.AddMinutes(-180), now.AddMinutes(-180),
            now.AddMinutes(-175), BlockExecutionStatusEnum.Failed, CurrentTaskId, 2);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, now.AddMinutes(-200), now.AddMinutes(-200), null,
            BlockExecutionStatusEnum.Started, CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, now.AddMinutes(-220), null, null,
            BlockExecutionStatusEnum.NotStarted, CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, now.AddMinutes(-240), now.AddMinutes(-240),
            now.AddMinutes(-235), BlockExecutionStatusEnum.Completed, CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block5, now.AddMinutes(-250), now.AddMinutes(-250), null,
            BlockExecutionStatusEnum.Started, CurrentTaskId, 3);
    }

    private void InsertObjectTestData(TaskDeathModeEnum taskDeathMode)
    {
        var now = DateTime.UtcNow;
        if (taskDeathMode == TaskDeathModeEnum.Override)
            _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, OneMinuteSpan,
                now.AddMinutes(-250), now.AddMinutes(-179));
        else
            _taskExecution1 = _executionsHelper.InsertKeepAliveTaskExecution(_taskDefinitionId, TwentySecondSpan,
                FiveMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));

        InsertObjectBlocksTestData();
    }

    private void InsertObjectBlocksTestData()
    {
        var now = DateTime.UtcNow;
        _block1 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-246), Guid.NewGuid().ToString(), CurrentTaskId);
        _block2 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-247), Guid.NewGuid().ToString(), CurrentTaskId);
        _block3 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-248), Guid.NewGuid().ToString(), CurrentTaskId);
        _block4 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-249), Guid.NewGuid().ToString(), CurrentTaskId);
        _block5 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-250), Guid.NewGuid().ToString(), CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, now.AddMinutes(-180), now.AddMinutes(-180),
            now.AddMinutes(-175), BlockExecutionStatusEnum.Failed, CurrentTaskId, 2);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, now.AddMinutes(-200), now.AddMinutes(-200), null,
            BlockExecutionStatusEnum.Started, CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, now.AddMinutes(-220), null, null,
            BlockExecutionStatusEnum.NotStarted, CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, now.AddMinutes(-240), now.AddMinutes(-240),
            now.AddMinutes(-235), BlockExecutionStatusEnum.Completed, CurrentTaskId);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block5, now.AddMinutes(-250), now.AddMinutes(-250), null,
            BlockExecutionStatusEnum.Started, CurrentTaskId, 3);
    }

    private FindDeadBlocksRequest CreateDeadBlockRequest(BlockTypeEnum blockType, TaskDeathModeEnum taskDeathMode,
        int blockCountLimit)
    {
        return CreateDeadBlockRequest(blockType, taskDeathMode, blockCountLimit, 3, -300);
    }

    private FindDeadBlocksRequest CreateDeadBlockRequest(BlockTypeEnum blockType, TaskDeathModeEnum taskDeathMode,
        int blockCountLimit, int attemptLimit, int fromMinutesBack)
    {
        return new FindDeadBlocksRequest(CurrentTaskId,
            1,
            blockType,
            DateTime.UtcNow.AddMinutes(fromMinutesBack),
            DateTime.UtcNow,
            blockCountLimit,
            taskDeathMode,
            attemptLimit);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndDateRange_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.DateRange, TaskDeathModeEnum.Override, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.DateRange, TaskDeathModeEnum.Override, blockCountLimit,
                3, -100);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 1;
            var request = CreateDeadBlockRequest(BlockTypeEnum.DateRange, TaskDeathModeEnum.Override, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 5;
            var retryLimit = 2;
            var request = CreateDeadBlockRequest(BlockTypeEnum.DateRange, TaskDeathModeEnum.Override, blockCountLimit,
                retryLimit,
                -300);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.DateRange, TaskDeathModeEnum.KeepAlive, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 2;
            var request = CreateDeadBlockRequest(BlockTypeEnum.DateRange, TaskDeathModeEnum.KeepAlive, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-2));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.DateRange, TaskDeathModeEnum.KeepAlive, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            var blockCountLimit = 5;
            var request =
                CreateDeadBlockRequest(BlockTypeEnum.DateRange, TaskDeathModeEnum.KeepAlive, blockCountLimit, 3, -100);

            // ACT
            var sut = _blockRepository;
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndNumericRange_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 5;
            var request =
                CreateDeadBlockRequest(BlockTypeEnum.NumericRange, TaskDeathModeEnum.Override, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 5;
            var request =
                CreateDeadBlockRequest(BlockTypeEnum.NumericRange, TaskDeathModeEnum.Override, blockCountLimit, 3,
                    -100);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 1;
            var request =
                CreateDeadBlockRequest(BlockTypeEnum.NumericRange, TaskDeathModeEnum.Override, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 5;
            var retryLimit = 2;
            var request = CreateDeadBlockRequest(BlockTypeEnum.NumericRange, TaskDeathModeEnum.Override,
                blockCountLimit,
                retryLimit, -300);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 5;
            var request =
                CreateDeadBlockRequest(BlockTypeEnum.NumericRange, TaskDeathModeEnum.KeepAlive, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 2;
            var request =
                CreateDeadBlockRequest(BlockTypeEnum.NumericRange, TaskDeathModeEnum.KeepAlive, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-2));

            var blockCountLimit = 5;
            var request =
                CreateDeadBlockRequest(BlockTypeEnum.NumericRange, TaskDeathModeEnum.KeepAlive, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.NumericRange, TaskDeathModeEnum.KeepAlive,
                blockCountLimit, 3,
                -100);

            // ACT
            var sut = _blockRepository;
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndList_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.List, TaskDeathModeEnum.Override, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 5;
            var request =
                CreateDeadBlockRequest(BlockTypeEnum.List, TaskDeathModeEnum.Override, blockCountLimit, 3, -100);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 1;
            var request = CreateDeadBlockRequest(BlockTypeEnum.List, TaskDeathModeEnum.Override, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 5;
            var attemptLimit = 2;
            var request =
                CreateDeadBlockRequest(BlockTypeEnum.List, TaskDeathModeEnum.Override, blockCountLimit, attemptLimit,
                    -300);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.List, TaskDeathModeEnum.KeepAlive, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 2;
            var request = CreateDeadBlockRequest(BlockTypeEnum.List, TaskDeathModeEnum.KeepAlive, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-2));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.List, TaskDeathModeEnum.KeepAlive, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertListTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.List, TaskDeathModeEnum.KeepAlive, blockCountLimit, 3,
                -100);

            // ACT
            var sut = _blockRepository;
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_OverrideModeAndObject_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.Object, TaskDeathModeEnum.Override, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.Object, TaskDeathModeEnum.Override, blockCountLimit, 3,
                -100);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 1;
            var request = CreateDeadBlockRequest(BlockTypeEnum.Object, TaskDeathModeEnum.Override, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathModeEnum.Override);
            var blockCountLimit = 5;
            var attemptLimit = 2;
            var request = CreateDeadBlockRequest(BlockTypeEnum.Object, TaskDeathModeEnum.Override, blockCountLimit,
                attemptLimit,
                -300);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.Object, TaskDeathModeEnum.KeepAlive, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            var blockCountLimit = 2;
            var request = CreateDeadBlockRequest(BlockTypeEnum.Object, TaskDeathModeEnum.KeepAlive, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-2));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.Object, TaskDeathModeEnum.KeepAlive, blockCountLimit);

            // ACT
            var sut = _blockRepository;
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
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathModeEnum.KeepAlive);
            _executionsHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            var blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockTypeEnum.Object, TaskDeathModeEnum.KeepAlive, blockCountLimit, 3,
                -100);

            // ACT
            var sut = _blockRepository;
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        });
    }
}