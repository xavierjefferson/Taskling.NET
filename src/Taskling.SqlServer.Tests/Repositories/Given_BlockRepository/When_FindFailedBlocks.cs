﻿using System;
using System.Threading.Tasks;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.Tasks;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Repositories.Given_BlockRepository;
[Collection(Constants.CollectionName)]
public class When_FindFailedBlocks
{
    private readonly BlocksHelper _blocksHelper;
    private readonly ExecutionsHelper _executionHelper;
    private readonly int _taskDefinitionId;

    public When_FindFailedBlocks()
    {
        _executionHelper = new ExecutionsHelper();
        _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);
        _blocksHelper = new BlocksHelper();
        _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);

        _taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        _executionHelper.InsertAvailableExecutionToken(_taskDefinitionId);
    }

    private BlockRepository CreateSut()
    {
        return new BlockRepository(new TaskRepository());
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedDateRangeBlocksExistInTargetPeriodAndNumberIsLessThanBlocksLimit_ThenReturnAllFailedBlocks()
    {
        // ARRANGE
        var now = DateTime.UtcNow;
        var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
            now.AddMinutes(-12), now.AddMinutes(-1));
        var block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-2), now.AddMinutes(-1));
        var block2 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-12), now.AddMinutes(-11));
        _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
            now.AddMinutes(-1), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
            now.AddMinutes(-11), BlockExecutionStatus.Completed);

        var request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            1,
            BlockType.DateRange,
            DateTime.UtcNow.AddMinutes(-20),
            DateTime.UtcNow,
            2,
            3);

        // ACT
        var sut = CreateSut();
        var failedBlocks = await sut.FindFailedRangeBlocksAsync(request);

        // ASSERT
        Assert.Equal(1, failedBlocks.Count);
        Assert.Equal(block1, failedBlocks[0].RangeBlockId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedDateRangeBlocksExistInTargetPeriodAndNumberIsGreaterThanBlocksLimit_ThenReturnOldestBlocksUpToCountLimit()
    {
        // ARRANGE
        var now = DateTime.UtcNow;
        var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
            now.AddMinutes(-32), now.AddMinutes(-1));
        var block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-2), now.AddMinutes(-1));
        var block2 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-12), now.AddMinutes(-11));
        var block3 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-22), now.AddMinutes(-21));
        var block4 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-32), now.AddMinutes(-31));
        _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
            now.AddMinutes(-1), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
            now.AddMinutes(-11), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block3, now.AddMinutes(-22), now.AddMinutes(-22),
            now.AddMinutes(-21), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block4, now.AddMinutes(-32), now.AddMinutes(-32),
            now.AddMinutes(-31), BlockExecutionStatus.Completed);

        var blockCountLimit = 2;

        var request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            1,
            BlockType.DateRange,
            DateTime.UtcNow.AddMinutes(-200),
            DateTime.UtcNow,
            blockCountLimit,
            3);

        // ACT
        var sut = CreateSut();
        var failedBlocks = await sut.FindFailedRangeBlocksAsync(request);

        // ASSERT
        Assert.Equal(blockCountLimit, failedBlocks.Count);
        Assert.Contains(failedBlocks, x => x.RangeBlockId == block2);
        Assert.Contains(failedBlocks, x => x.RangeBlockId == block3);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_FailedDateRangeBlocksExistOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        var now = DateTime.UtcNow;
        var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
            now.AddMinutes(-212), now.AddMinutes(-200));
        var block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-200), now.AddMinutes(-201));
        var block2 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-212), now.AddMinutes(-211));
        _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-200), now.AddMinutes(-200),
            now.AddMinutes(-201), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-212), now.AddMinutes(-212),
            now.AddMinutes(-211), BlockExecutionStatus.Completed);

        var request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            1,
            BlockType.DateRange,
            DateTime.UtcNow.AddMinutes(-20),
            DateTime.UtcNow,
            2,
            3);

        // ACT
        var sut = CreateSut();
        var failedBlocks = await sut.FindFailedRangeBlocksAsync(request);

        // ASSERT
        Assert.Equal(0, failedBlocks.Count);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedNumericRangeBlocksExistInTargetPeriodAndNumberIsLessThanBlocksLimit_ThenReturnAllFailedBlocks()
    {
        // ARRANGE
        var now = DateTime.UtcNow;
        var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
            now.AddMinutes(-12), now.AddMinutes(-1));
        var block1 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 1, 2, now.AddMinutes(-2));
        var block2 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 3, 4, now.AddMinutes(-12));
        _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
            now.AddMinutes(-1), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
            now.AddMinutes(-11), BlockExecutionStatus.Completed);

        var request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            1,
            BlockType.NumericRange,
            DateTime.UtcNow.AddMinutes(-20),
            DateTime.UtcNow,
            2,
            3);

        // ACT
        var sut = CreateSut();
        var failedBlocks = await sut.FindFailedRangeBlocksAsync(request);

        // ASSERT
        Assert.Equal(1, failedBlocks.Count);
        Assert.Equal(block1, failedBlocks[0].RangeBlockId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedNumericRangeBlocksExistInTargetPeriodAndNumberIsGreaterThanBlocksLimit_ThenReturnOldestBlocksUpToCountLimit()
    {
        // ARRANGE
        var now = DateTime.UtcNow;
        var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
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

        var request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            1,
            BlockType.NumericRange,
            DateTime.UtcNow.AddMinutes(-200),
            DateTime.UtcNow,
            blockCountLimit,
            3);

        // ACT
        var sut = CreateSut();
        var failedBlocks = await sut.FindFailedRangeBlocksAsync(request);

        // ASSERT
        Assert.Equal(blockCountLimit, failedBlocks.Count);
        Assert.Contains(failedBlocks, x => x.RangeBlockId == block2);
        Assert.Contains(failedBlocks, x => x.RangeBlockId == block3);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_FailedNumericRangeBlocksExistOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        var now = DateTime.UtcNow;
        var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
            now.AddMinutes(-212), now.AddMinutes(-200));
        var block1 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 1, 2, now.AddMinutes(-200));
        var block2 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 3, 4, now.AddMinutes(-212));
        _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-200), now.AddMinutes(-200),
            now.AddMinutes(-201), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-212), now.AddMinutes(-212),
            now.AddMinutes(-211), BlockExecutionStatus.Completed);

        var request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            1,
            BlockType.NumericRange,
            DateTime.UtcNow.AddMinutes(-20),
            DateTime.UtcNow,
            2,
            3);

        // ACT
        var sut = CreateSut();
        var failedBlocks = await sut.FindFailedRangeBlocksAsync(request);

        // ASSERT
        Assert.Equal(0, failedBlocks.Count);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_FailedListBlocksExistInTargetPeriodAndNumberIsLessThanBlocksLimit_ThenReturnAllFailedBlocks()
    {
        // ARRANGE
        var now = DateTime.UtcNow;
        var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
            now.AddMinutes(-12), now.AddMinutes(-1));
        var block1 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-2));
        var block2 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-12));
        _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
            now.AddMinutes(-1), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
            now.AddMinutes(-11), BlockExecutionStatus.Completed);

        var request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            1,
            BlockType.List,
            DateTime.UtcNow.AddMinutes(-20),
            DateTime.UtcNow,
            2,
            3);

        // ACT
        var sut = CreateSut();
        var failedBlocks = await sut.FindFailedListBlocksAsync(request);

        // ASSERT
        Assert.Equal(1, failedBlocks.Count);
        Assert.Equal(block1, failedBlocks[0].ListBlockId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedListBlocksExistInTargetPeriodAndNumberIsGreaterThanBlocksLimit_ThenReturnOldestBlocksUpToCountLimit()
    {
        // ARRANGE
        var now = DateTime.UtcNow;
        var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
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

        var request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            1,
            BlockType.List,
            DateTime.UtcNow.AddMinutes(-200),
            DateTime.UtcNow,
            blockCountLimit,
            3);

        // ACT
        var sut = CreateSut();
        var failedBlocks = await sut.FindFailedListBlocksAsync(request);

        // ASSERT
        Assert.Equal(blockCountLimit, failedBlocks.Count);
        Assert.Contains(failedBlocks, x => x.ListBlockId == block2);
        Assert.Contains(failedBlocks, x => x.ListBlockId == block3);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_FailedListBlocksExistOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        var now = DateTime.UtcNow;
        var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
            now.AddMinutes(-212), now.AddMinutes(-200));
        var block1 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-200));
        var block2 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-212));
        _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-200), now.AddMinutes(-200),
            now.AddMinutes(-201), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-212), now.AddMinutes(-212),
            now.AddMinutes(-211), BlockExecutionStatus.Completed);

        var request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            1,
            BlockType.List,
            DateTime.UtcNow.AddMinutes(-20),
            DateTime.UtcNow,
            2,
            3);

        // ACT
        var sut = CreateSut();
        var failedBlocks = await sut.FindFailedListBlocksAsync(request);

        // ASSERT
        Assert.Equal(0, failedBlocks.Count);
    }


    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedObjectBlocksExistInTargetPeriodAndNumberIsLessThanBlocksLimit_ThenReturnAllFailedBlocks()
    {
        // ARRANGE
        var now = DateTime.UtcNow;
        var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
            now.AddMinutes(-12), now.AddMinutes(-1));
        var block1 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-2), Guid.NewGuid().ToString());
        var block2 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-12), Guid.NewGuid().ToString());
        _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
            now.AddMinutes(-1), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
            now.AddMinutes(-11), BlockExecutionStatus.Completed);

        var request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            1,
            BlockType.Object,
            DateTime.UtcNow.AddMinutes(-20),
            DateTime.UtcNow,
            2,
            3);

        // ACT
        var sut = CreateSut();
        var failedBlocks = await sut.FindFailedObjectBlocksAsync<string>(request);

        // ASSERT
        Assert.Equal(1, failedBlocks.Count);
        Assert.Equal(block1, failedBlocks[0].ObjectBlockId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        When_FailedObjectBlocksExistInTargetPeriodAndNumberIsGreaterThanBlocksLimit_ThenReturnOldestBlocksUpToCountLimit()
    {
        // ARRANGE
        var now = DateTime.UtcNow;
        var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
            now.AddMinutes(-32), now.AddMinutes(-1));
        var block1 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-2), Guid.NewGuid().ToString());
        var block2 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-12), Guid.NewGuid().ToString());
        var block3 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-22), Guid.NewGuid().ToString());
        var block4 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-32), Guid.NewGuid().ToString());
        _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-2), now.AddMinutes(-2),
            now.AddMinutes(-1), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-12), now.AddMinutes(-12),
            now.AddMinutes(-11), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block3, now.AddMinutes(-22), now.AddMinutes(-22),
            now.AddMinutes(-21), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block4, now.AddMinutes(-32), now.AddMinutes(-32),
            now.AddMinutes(-31), BlockExecutionStatus.Completed);

        var blockCountLimit = 2;

        var request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            1,
            BlockType.Object,
            DateTime.UtcNow.AddMinutes(-200),
            DateTime.UtcNow,
            blockCountLimit,
            3);

        // ACT
        var sut = CreateSut();
        var failedBlocks = await sut.FindFailedObjectBlocksAsync<string>(request);

        // ASSERT
        Assert.Equal(blockCountLimit, failedBlocks.Count);
        Assert.Contains(failedBlocks, x => x.ObjectBlockId == block2);
        Assert.Contains(failedBlocks, x => x.ObjectBlockId == block3);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task When_FailedObjectBlocksExistOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        var now = DateTime.UtcNow;
        var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, new TimeSpan(0, 1, 0),
            now.AddMinutes(-212), now.AddMinutes(-200));
        var block1 =
            _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-200), Guid.NewGuid().ToString());
        var block2 =
            _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-212), Guid.NewGuid().ToString());
        _blocksHelper.InsertBlockExecution(taskExecution1, block1, now.AddMinutes(-200), now.AddMinutes(-200),
            now.AddMinutes(-201), BlockExecutionStatus.Failed);
        _blocksHelper.InsertBlockExecution(taskExecution1, block2, now.AddMinutes(-212), now.AddMinutes(-212),
            now.AddMinutes(-211), BlockExecutionStatus.Completed);

        var request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            1,
            BlockType.Object,
            DateTime.UtcNow.AddMinutes(-20),
            DateTime.UtcNow,
            2,
            3);

        // ACT
        var sut = CreateSut();
        var failedBlocks = await sut.FindFailedObjectBlocksAsync<string>(request);

        // ASSERT
        Assert.Equal(0, failedBlocks.Count);
    }
}