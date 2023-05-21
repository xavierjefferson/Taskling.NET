using System;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Repositories.Given_BlockRepository;

[Collection(TestConstants.CollectionName)]
public class When_FindFailedBlocks : TestBase
{
    private readonly IBlockRepository _blockRepository;
    private readonly IBlocksHelper _blocksHelper;
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_FindFailedBlocks> _logger;
    private readonly long _taskDefinitionId;

    public When_FindFailedBlocks(IBlocksHelper blocksHelper, IBlockRepository blockRepository,
        IExecutionsHelper executionsHelper, IClientHelper clientHelper, ILogger<When_FindFailedBlocks> logger,
        ITaskRepository taskRepository) : base(executionsHelper)
    {
        _logger = logger;
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _executionsHelper = executionsHelper;
        _executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);
        _blocksHelper = blocksHelper;
        _clientHelper = clientHelper;

        _blockRepository = blockRepository;
        _blocksHelper.DeleteBlocks(CurrentTaskId.ApplicationName);

        _taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
        _executionsHelper.InsertAvailableExecutionToken(_taskDefinitionId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedDateRangeBlocksExistInTargetPeriodAndNumberIsLessThanBlocksLimit_ThenReturnAllFailedBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var now = DateTime.UtcNow;
            var taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
                now.AddMinutes(-12), now.AddMinutes(-1));
            var block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-2), now.AddMinutes(-1));
            var block2 =
                _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-12), now.AddMinutes(-11));
            _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
                now.AddMinutes(-1), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
                now.AddMinutes(-11), BlockExecutionStatus.Completed);

            var request = new FindFailedBlocksRequest(CurrentTaskId,
                1,
                BlockType.DateRange,
                DateTime.UtcNow.AddMinutes(-20),
                DateTime.UtcNow,
                2,
                3);

            // ACT
            var sut = _blockRepository;
            var failedBlocks = await sut.FindFailedRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(1, failedBlocks.Count);
            Assert.Equal(block1, failedBlocks[0].RangeBlockId);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedDateRangeBlocksExistInTargetPeriodAndNumberIsGreaterThanBlocksLimit_ThenReturnOldestBlocksUpToCountLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var now = DateTime.UtcNow;
            var taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
                now.AddMinutes(-32), now.AddMinutes(-1));
            var block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-2), now.AddMinutes(-1));
            var block2 =
                _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-12), now.AddMinutes(-11));
            var block3 =
                _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-22), now.AddMinutes(-21));
            var block4 =
                _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-32), now.AddMinutes(-31));
            _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
                now.AddMinutes(-1), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
                now.AddMinutes(-11), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block3, now.AddMinutes(-22), now.AddMinutes(-22),
                now.AddMinutes(-21), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block4, now.AddMinutes(-32), now.AddMinutes(-32),
                now.AddMinutes(-31), BlockExecutionStatus.Completed);

            var blockCountLimit = 2;

            var request = new FindFailedBlocksRequest(CurrentTaskId,
                1,
                BlockType.DateRange,
                DateTime.UtcNow.AddMinutes(-200),
                DateTime.UtcNow,
                blockCountLimit,
                3);

            // ACT
            var sut = _blockRepository;
            var failedBlocks = await sut.FindFailedRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(blockCountLimit, failedBlocks.Count);
            Assert.Contains(failedBlocks, x => x.RangeBlockId == block2);
            Assert.Contains(failedBlocks, x => x.RangeBlockId == block3);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_FailedDateRangeBlocksExistOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var now = DateTime.UtcNow;
            var taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
                now.AddMinutes(-212), now.AddMinutes(-200));
            var block1 =
                _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-200), now.AddMinutes(-201));
            var block2 =
                _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-212), now.AddMinutes(-211));
            _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-200), now.AddMinutes(-200),
                now.AddMinutes(-201), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-212), now.AddMinutes(-212),
                now.AddMinutes(-211), BlockExecutionStatus.Completed);

            var request = new FindFailedBlocksRequest(CurrentTaskId,
                1,
                BlockType.DateRange,
                DateTime.UtcNow.AddMinutes(-20),
                DateTime.UtcNow,
                2,
                3);

            // ACT
            var sut = _blockRepository;
            var failedBlocks = await sut.FindFailedRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, failedBlocks.Count);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedNumericRangeBlocksExistInTargetPeriodAndNumberIsLessThanBlocksLimit_ThenReturnAllFailedBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var now = DateTime.UtcNow;
            var taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
                now.AddMinutes(-12), now.AddMinutes(-1));
            var block1 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 1, 2, now.AddMinutes(-2));
            var block2 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 3, 4, now.AddMinutes(-12));
            _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
                now.AddMinutes(-1), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
                now.AddMinutes(-11), BlockExecutionStatus.Completed);

            var request = new FindFailedBlocksRequest(CurrentTaskId,
                1,
                BlockType.NumericRange,
                DateTime.UtcNow.AddMinutes(-20),
                DateTime.UtcNow,
                2,
                3);

            // ACT
            var sut = _blockRepository;
            var failedBlocks = await sut.FindFailedRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(1, failedBlocks.Count);
            Assert.Equal(block1, failedBlocks[0].RangeBlockId);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedNumericRangeBlocksExistInTargetPeriodAndNumberIsGreaterThanBlocksLimit_ThenReturnOldestBlocksUpToCountLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var now = DateTime.UtcNow;
            var taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
                now.AddMinutes(-32), now.AddMinutes(-1));
            var block1 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 1, 2, now.AddMinutes(-2));
            var block2 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 3, 4, now.AddMinutes(-12));
            var block3 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 5, 6, now.AddMinutes(-22));
            var block4 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 7, 8, now.AddMinutes(-32));
            _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
                now.AddMinutes(-1), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
                now.AddMinutes(-11), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block3, now.AddMinutes(-22), now.AddMinutes(-22),
                now.AddMinutes(-21), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block4, now.AddMinutes(-32), now.AddMinutes(-32),
                now.AddMinutes(-31), BlockExecutionStatus.Completed);

            var blockCountLimit = 2;

            var request = new FindFailedBlocksRequest(CurrentTaskId,
                1,
                BlockType.NumericRange,
                DateTime.UtcNow.AddMinutes(-200),
                DateTime.UtcNow,
                blockCountLimit,
                3);

            // ACT
            var sut = _blockRepository;
            var failedBlocks = await sut.FindFailedRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(blockCountLimit, failedBlocks.Count);
            Assert.Contains(failedBlocks, x => x.RangeBlockId == block2);
            Assert.Contains(failedBlocks, x => x.RangeBlockId == block3);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_FailedNumericRangeBlocksExistOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var now = DateTime.UtcNow;
            var taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
                now.AddMinutes(-212), now.AddMinutes(-200));
            var block1 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 1, 2, now.AddMinutes(-200));
            var block2 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 3, 4, now.AddMinutes(-212));
            _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-200), now.AddMinutes(-200),
                now.AddMinutes(-201), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-212), now.AddMinutes(-212),
                now.AddMinutes(-211), BlockExecutionStatus.Completed);

            var request = new FindFailedBlocksRequest(CurrentTaskId,
                1,
                BlockType.NumericRange,
                DateTime.UtcNow.AddMinutes(-20),
                DateTime.UtcNow,
                2,
                3);

            // ACT
            var sut = _blockRepository;
            var failedBlocks = await sut.FindFailedRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, failedBlocks.Count);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_FailedListBlocksExistInTargetPeriodAndNumberIsLessThanBlocksLimit_ThenReturnAllFailedBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var now = DateTime.UtcNow;
            var taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
                now.AddMinutes(-12), now.AddMinutes(-1));
            var block1 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-2));
            var block2 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-12));
            _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
                now.AddMinutes(-1), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
                now.AddMinutes(-11), BlockExecutionStatus.Completed);

            var request = new FindFailedBlocksRequest(CurrentTaskId,
                1,
                BlockType.List,
                DateTime.UtcNow.AddMinutes(-20),
                DateTime.UtcNow,
                2,
                3);

            // ACT
            var sut = _blockRepository;
            var failedBlocks = await sut.FindFailedListBlocksAsync(request);

            // ASSERT
            Assert.Equal(1, failedBlocks.Count);
            Assert.Equal(block1, failedBlocks[0].ListBlockId);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedListBlocksExistInTargetPeriodAndNumberIsGreaterThanBlocksLimit_ThenReturnOldestBlocksUpToCountLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var now = DateTime.UtcNow;
            var taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
                now.AddMinutes(-32), now.AddMinutes(-1));
            var block1 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-2));
            var block2 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-12));
            var block3 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-22));
            var block4 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-32));
            _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
                now.AddMinutes(-1), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
                now.AddMinutes(-11), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block3, now.AddMinutes(-22), now.AddMinutes(-22),
                now.AddMinutes(-21), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block4, now.AddMinutes(-32), now.AddMinutes(-32),
                now.AddMinutes(-31), BlockExecutionStatus.Completed);

            var blockCountLimit = 2;

            var request = new FindFailedBlocksRequest(CurrentTaskId,
                1,
                BlockType.List,
                DateTime.UtcNow.AddMinutes(-200),
                DateTime.UtcNow,
                blockCountLimit,
                3);

            // ACT
            var sut = _blockRepository;
            var failedBlocks = await sut.FindFailedListBlocksAsync(request);

            // ASSERT
            Assert.Equal(blockCountLimit, failedBlocks.Count);
            Assert.Contains(failedBlocks, x => x.ListBlockId == block2);
            Assert.Contains(failedBlocks, x => x.ListBlockId == block3);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_FailedListBlocksExistOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var now = DateTime.UtcNow;
            var taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
                now.AddMinutes(-212), now.AddMinutes(-200));
            var block1 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-200));
            var block2 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-212));
            _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-200), now.AddMinutes(-200),
                now.AddMinutes(-201), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-212), now.AddMinutes(-212),
                now.AddMinutes(-211), BlockExecutionStatus.Completed);

            var request = new FindFailedBlocksRequest(CurrentTaskId,
                1,
                BlockType.List,
                DateTime.UtcNow.AddMinutes(-20),
                DateTime.UtcNow,
                2,
                3);

            // ACT
            var sut = _blockRepository;
            var failedBlocks = await sut.FindFailedListBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, failedBlocks.Count);
        });
    }


    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedObjectBlocksExistInTargetPeriodAndNumberIsLessThanBlocksLimit_ThenReturnAllFailedBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var now = DateTime.UtcNow;
            var taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
                now.AddMinutes(-12), now.AddMinutes(-1));
            var block1 =
                _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-2), Guid.NewGuid().ToString());
            var block2 =
                _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-12), Guid.NewGuid().ToString());
            _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
                now.AddMinutes(-1), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
                now.AddMinutes(-11), BlockExecutionStatus.Completed);

            var request = new FindFailedBlocksRequest(CurrentTaskId,
                1,
                BlockType.Object,
                DateTime.UtcNow.AddMinutes(-20),
                DateTime.UtcNow,
                2,
                3);

            // ACT
            var sut = _blockRepository;
            var failedBlocks = await sut.FindFailedObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(1, failedBlocks.Count);
            Assert.Equal(block1, failedBlocks[0].ObjectBlockId);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedObjectBlocksExistInTargetPeriodAndNumberIsGreaterThanBlocksLimit_ThenReturnOldestBlocksUpToCountLimit()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var now = DateTime.UtcNow;
            var taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
                now.AddMinutes(-32), now.AddMinutes(-1));
            var block1 =
                _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-2), Guid.NewGuid().ToString());
            var block2 =
                _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-12), Guid.NewGuid().ToString());
            var block3 =
                _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-22), Guid.NewGuid().ToString());
            var block4 =
                _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-32), Guid.NewGuid().ToString());
            _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
                now.AddMinutes(-1), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
                now.AddMinutes(-11), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block3, now.AddMinutes(-22), now.AddMinutes(-22),
                now.AddMinutes(-21), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block4, now.AddMinutes(-32), now.AddMinutes(-32),
                now.AddMinutes(-31), BlockExecutionStatus.Completed);

            var blockCountLimit = 2;

            var request = new FindFailedBlocksRequest(CurrentTaskId,
                1,
                BlockType.Object,
                DateTime.UtcNow.AddMinutes(-200),
                DateTime.UtcNow,
                blockCountLimit,
                3);

            // ACT
            var sut = _blockRepository;
            var failedBlocks = await sut.FindFailedObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(blockCountLimit, failedBlocks.Count);
            Assert.Contains(failedBlocks, x => x.ObjectBlockId == block2);
            Assert.Contains(failedBlocks, x => x.ObjectBlockId == block3);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_FailedObjectBlocksExistOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var now = DateTime.UtcNow;
            var taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
                now.AddMinutes(-212), now.AddMinutes(-200));
            var block1 =
                _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-200), Guid.NewGuid().ToString());
            var block2 =
                _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-212), Guid.NewGuid().ToString());
            _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-200), now.AddMinutes(-200),
                now.AddMinutes(-201), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-212), now.AddMinutes(-212),
                now.AddMinutes(-211), BlockExecutionStatus.Completed);

            var request = new FindFailedBlocksRequest(CurrentTaskId,
                1,
                BlockType.Object,
                DateTime.UtcNow.AddMinutes(-20),
                DateTime.UtcNow,
                2,
                3);

            // ACT
            var sut = _blockRepository;
            var failedBlocks = await sut.FindFailedObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(0, failedBlocks.Count);
        });
    }
}