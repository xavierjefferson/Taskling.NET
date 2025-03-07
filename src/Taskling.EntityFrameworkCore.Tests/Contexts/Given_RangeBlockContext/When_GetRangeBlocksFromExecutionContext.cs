﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Taskling.Blocks.RangeBlocks;
using Taskling.Contexts;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.Enums;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Contexts.Given_RangeBlockContext;

[Collection(CollectionName)]
public class When_GetRangeBlocksFromExecutionContext : TestBase
{
    private readonly IBlocksHelper _blocksHelper;
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_GetRangeBlocksFromExecutionContext> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly long _taskDefinitionId;

    public When_GetRangeBlocksFromExecutionContext(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper,
        IClientHelper clientHelper, ILogger<When_GetRangeBlocksFromExecutionContext> logger,
        ILoggerFactory loggerFactory) : base(executionsHelper)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
        _blocksHelper = blocksHelper;
        _clientHelper = clientHelper;

        _blocksHelper.DeleteBlocks(CurrentTaskId);
        _executionsHelper = executionsHelper;
        _executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);

        _taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
        _executionsHelper.InsertAvailableExecutionToken(_taskDefinitionId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsDateRange_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var blockCountLimit = 10;

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext(blockCountLimit))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = TimeSpans.ThirtyMinutes;
                    var rangeBlocks =
                        await executionContext.GetDateRangeBlocksAsync(
                            x => x.WithRange(fromDate, toDate, maxBlockRange));
                    Assert.Equal(10, _blocksHelper.GetBlockCount(CurrentTaskId));
                    var expectedNotStartedCount = blockCountLimit;
                    var expectedCompletedCount = 0;
                    Assert.Equal(expectedNotStartedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.NotStarted));
                    Assert.Equal(0,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.Started));
                    Assert.Equal(expectedCompletedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.Completed));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        expectedNotStartedCount--;
                        Assert.Equal(expectedNotStartedCount,
                            _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                                BlockExecutionStatusEnum.NotStarted));
                        Assert.Equal(1,
                            _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                                BlockExecutionStatusEnum.Started));
                        // processing here
                        await rangeBlock.CompleteAsync();
                        expectedCompletedCount++;
                        Assert.Equal(expectedCompletedCount,
                            _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                                BlockExecutionStatusEnum.Completed));
                    }
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsDateRangeNoBlockNeeded_ThenEmptyListAndEventPersisted()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow;
                    var toDate = DateTime.UtcNow.AddHours(-12);
                    var maxBlockRange = TimeSpans.ThirtyMinutes;
                    var rangeBlocks =
                        await executionContext.GetDateRangeBlocksAsync(
                            x => x.WithRange(fromDate, toDate, maxBlockRange));
                    Assert.Equal(0, _blocksHelper.GetBlockCount(CurrentTaskId));

                    var lastEvent = _executionsHelper.GetLastEvent(_taskDefinitionId);
                    Assert.Equal(EventTypeEnum.CheckPoint, lastEvent.EventType);
                    Assert.Equal("No values for generate the block. Emtpy Block context returned.", lastEvent.Message);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsNumericRangeNoBlockNeeded_ThenEmptyListAndEventPersisted()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromNumber = 1000;
                    var toNumber = 800;
                    var maxBlockRange = 100;
                    var rangeBlocks =
                        await executionContext.GetNumericRangeBlocksAsync(x =>
                            x.WithRange(fromNumber, toNumber, maxBlockRange));
                    Assert.Equal(0, _blocksHelper.GetBlockCount(CurrentTaskId));

                    var lastEvent = _executionsHelper.GetLastEvent(_taskDefinitionId);
                    Assert.Equal(EventTypeEnum.CheckPoint, lastEvent.EventType);
                    Assert.Equal("No values for generate the block. Emtpy Block context returned.", lastEvent.Message);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsNumericRange_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var blockCountLimit = 10;

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext(blockCountLimit))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromNumber = 1000;
                    var toNumber = 3000;
                    var maxBlockRange = 100;
                    var blocks =
                        await executionContext.GetNumericRangeBlocksAsync(x =>
                            x.WithRange(fromNumber, toNumber, maxBlockRange));
                    Assert.Equal(10, _blocksHelper.GetBlockCount(CurrentTaskId));
                    var expectedNotStartedCount = blockCountLimit;
                    var expectedCompletedCount = 0;
                    Assert.Equal(expectedNotStartedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.NotStarted));
                    Assert.Equal(0,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.Started));
                    Assert.Equal(expectedCompletedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.Completed));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        expectedNotStartedCount--;
                        Assert.Equal(expectedNotStartedCount,
                            _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                                BlockExecutionStatusEnum.NotStarted));
                        Assert.Equal(1,
                            _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                                BlockExecutionStatusEnum.Started));

                        // processing here
                        await block.CompleteAsync();
                        expectedCompletedCount++;
                        Assert.Equal(expectedCompletedCount,
                            _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                                BlockExecutionStatusEnum.Completed));
                    }
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsNumericRange_BlocksDoNotShareIds()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromNumber = 0;
                    var toNumber = 100;
                    var maxBlockRange = 10;
                    var blocks =
                        await executionContext.GetNumericRangeBlocksAsync(x =>
                            x.WithRange(fromNumber, toNumber, maxBlockRange));

                    var counter = 0;
                    INumericRangeBlockContext lastBlock = null;
                    foreach (var block in blocks)
                    {
                        if (counter > 0)
                            Assert.Equal(lastBlock.NumericRangeBlock.EndNumber + 1,
                                block.NumericRangeBlock.StartNumber);

                        lastBlock = block;
                        counter++;
                    }
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsDateRange_PreviousBlock_ThenLastBlockContainsDates()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x
                        .WithRange(DateTimeHelper.CreateUtcDate(2016, 1, 1),
                            DateTimeHelper.CreateUtcDate(2016, 1, 31, 23, 59, 59, 999).AddMilliseconds(-1),
                            TimeSpans.OneDay)
                        .OverrideConfiguration()
                        .WithMaximumBlocksToGenerate(50));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            IDateRangeBlock expectedLastBlock = new RangeBlock(0, 1, DateTimeHelper.CreateUtcDate(2016, 1, 31).Ticks,
                DateTimeHelper.CreateUtcDate(2016, 1, 31, 23, 59, 59, 997).Ticks, BlockTypeEnum.DateRange,
                _loggerFactory.CreateLogger<RangeBlock>());

            // ACT
            IDateRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                    lastBlock = await executionContext.GetLastDateRangeBlockAsync(LastBlockOrderEnum.LastCreated);
            }

            // ASSERT
            AssertSimilarDates(expectedLastBlock.StartDate, lastBlock.StartDate);
            AssertSimilarDates(expectedLastBlock.EndDate, lastBlock.EndDate);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsDateRange_NoPreviousBlock_ThenLastBlockIsNull()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            IDateRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                    lastBlock = await executionContext.GetLastDateRangeBlockAsync(LastBlockOrderEnum.LastCreated);
            }

            // ASSERT
            Assert.Null(lastBlock);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsDateRange_PreviousBlockIsPhantom_ThenLastBlockIsNotThePhantom()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x
                        .WithRange(DateTimeHelper.CreateUtcDate(2016, 1, 1), DateTimeHelper.CreateUtcDate(2016, 1, 2), TimeSpans.TwoDays)
                        .OverrideConfiguration()
                        .WithMaximumBlocksToGenerate(50));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            _blocksHelper.InsertPhantomDateRangeBlock(CurrentTaskId,
                DateTimeHelper.CreateUtcDate(2015, 1, 1), DateTimeHelper.CreateUtcDate(2015, 1, 2));

            // ACT
            IDateRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                    lastBlock = await executionContext.GetLastDateRangeBlockAsync(LastBlockOrderEnum.LastCreated);
            }

            // ASSERT
            AssertSimilarDates(DateTimeHelper.CreateUtcDate(2016, 1, 1), lastBlock.StartDate);
            AssertSimilarDates(DateTimeHelper.CreateUtcDate(2016, 1, 2), lastBlock.EndDate);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsNumericRange_PreviousBlock_ThenLastBlockContainsDates()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(1, 1000, 100));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            var expectedLastBlock = new RangeBlock(0, 1, 901, 1000, BlockTypeEnum.NumericRange,
                _loggerFactory.CreateLogger<RangeBlock>());

            // ACT
            INumericRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                    lastBlock = await executionContext.GetLastNumericRangeBlockAsync(LastBlockOrderEnum
                        .MaxRangeStartValue);
            }

            // ASSERT
            Assert.Equal(expectedLastBlock.RangeBeginAsInt(), lastBlock.StartNumber);
            Assert.Equal(expectedLastBlock.RangeEndAsInt(), lastBlock.EndNumber);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsNumericRange_NoPreviousBlock_ThenLastBlockIsNull()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            INumericRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                    lastBlock = await executionContext.GetLastNumericRangeBlockAsync(LastBlockOrderEnum.LastCreated);
            }

            // ASSERT
            Assert.Null(lastBlock);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsNumericRange_PreviousBlockIsPhantom_ThenLastBlockIsNotThePhantom()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks =
                        await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(1000, 2000, 2000));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            _blocksHelper.InsertPhantomNumericBlock(CurrentTaskId, 0, 100);

            // ACT
            INumericRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                    lastBlock = await executionContext.GetLastNumericRangeBlockAsync(LastBlockOrderEnum.LastCreated);
            }

            // ASSERT
            Assert.Equal(1000, (int)lastBlock.StartNumber);
            Assert.Equal(2000, (int)lastBlock.EndNumber);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsDateRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackTheFailedBlockWhenRequested()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync(referenceValue);
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = TimeSpans.ThirtyMinutes;
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x
                        .WithRange(fromDate, toDate, maxBlockRange)
                        .OverrideConfiguration()
                        .WithMaximumBlocksToGenerate(5));

                    await rangeBlocks[0].StartAsync();
                    await rangeBlocks[0].CompleteAsync(); // completed
                    await rangeBlocks[1].StartAsync();
                    await rangeBlocks[1].FailedAsync("Something bad happened"); // failed
                    // 2 not started
                    await rangeBlocks[3].StartAsync(); // started
                    await rangeBlocks[4].StartAsync();
                    await rangeBlocks[4].CompleteAsync(); // completed
                }
            }

            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithReprocessDateRange()
                        .PendingAndFailedBlocks()
                        .OfExecutionWith(referenceValue));

                    Assert.Equal(3, rangeBlocks.Count);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsDateRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackAllBlocksWhenRequested()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync(referenceValue);
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = TimeSpans.ThirtyMinutes;
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x
                        .WithRange(fromDate, toDate, maxBlockRange)
                        .OverrideConfiguration()
                        .WithMaximumBlocksToGenerate(5));

                    await rangeBlocks[0].StartAsync();
                    await rangeBlocks[0].CompleteAsync(); // completed
                    await rangeBlocks[1].StartAsync();
                    await rangeBlocks[1].FailedAsync(); // failed
                    // 2 not started
                    await rangeBlocks[3].StartAsync(); // started
                    await rangeBlocks[4].StartAsync();
                    await rangeBlocks[4].CompleteAsync(); // completed
                }
            }

            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithReprocessDateRange()
                        .AllBlocks()
                        .OfExecutionWith(referenceValue));

                    Assert.Equal(5, rangeBlocks.Count);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsNumericRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackTheFailedBlockWhenRequested()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync(referenceValue);
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromNumber = 1000;
                    var toNumber = 3000;
                    var maxBlockRange = 100;
                    var blocks = await executionContext.GetNumericRangeBlocksAsync(x => x
                        .WithRange(fromNumber, toNumber, maxBlockRange)
                        .OverrideConfiguration()
                        .WithMaximumBlocksToGenerate(5));

                    await blocks[0].StartAsync();
                    await blocks[0].CompleteAsync(); // completed
                    await blocks[1].StartAsync();
                    await blocks[1].FailedAsync(); // failed
                    // 2 not started
                    await blocks[3].StartAsync(); // started
                    await blocks[4].StartAsync();
                    await blocks[4].CompleteAsync(); // completed
                }
            }

            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithReprocessNumericRange()
                        .PendingAndFailedBlocks()
                        .OfExecutionWith(referenceValue));

                    Assert.Equal(3, rangeBlocks.Count);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsNumericRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackAllBlocksWhenRequested()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync(referenceValue);
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromNumber = 1000;
                    var toNumber = 3000;
                    var maxBlockRange = 100;
                    var blocks = await executionContext.GetNumericRangeBlocksAsync(x => x
                        .WithRange(fromNumber, toNumber, maxBlockRange)
                        .OverrideConfiguration()
                        .WithMaximumBlocksToGenerate(5));

                    await blocks[0].StartAsync();
                    await blocks[0].CompleteAsync(); // completed
                    await blocks[1].StartAsync();
                    await blocks[1].FailedAsync(); // failed
                    // 2 not started
                    await blocks[3].StartAsync(); // started
                    await blocks[4].StartAsync();
                    await blocks[4].CompleteAsync(); // completed
                }
            }

            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithReprocessNumericRange()
                        .AllBlocks()
                        .OfExecutionWith(referenceValue));

                    Assert.Equal(5, rangeBlocks.Count);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsDateRangeWithPreviousDeadBlocks_ThenReprocessOk()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            await CreateFailedDateTaskAsync();
            await CreateDeadDateTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var from = DateTimeHelper.CreateUtcDate(2016, 1, 7);
                    var to = DateTimeHelper.CreateUtcDate(2016, 1, 7);
                    var maxBlockSize = TimeSpans.OneDay;
                    var dateBlocks = await executionContext.GetDateRangeBlocksAsync(x => x
                        .WithRange(from, to, maxBlockSize)
                        .OverrideConfiguration()
                        .WithReprocessDeadTasks(TimeSpans.OneDay, 3)
                        .WithReprocessFailedTasks(TimeSpans.OneDay, 3)
                        .WithMaximumBlocksToGenerate(8));

                    var counter = 0;
                    foreach (var block in dateBlocks)
                    {
                        await block.StartAsync();

                        await block.CompleteAsync();

                        counter++;
                        Assert.Equal(counter,
                            _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                                BlockExecutionStatusEnum.Completed));
                    }
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsDateRangeWithOverridenConfiguration_ThenOverridenValuesAreUsed()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            await CreateFailedDateTaskAsync();
            await CreateDeadDateTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var from = DateTimeHelper.CreateUtcDate(2016, 1, 7);
                    var to = DateTimeHelper.CreateUtcDate(2016, 1, 31);
                    var maxBlockSize = TimeSpans.OneDay;
                    var dateBlocks = await executionContext.GetDateRangeBlocksAsync(x => x
                        .WithRange(from, to, maxBlockSize)
                        .OverrideConfiguration()
                        .WithReprocessDeadTasks(TimeSpans.OneDay, 3)
                        .WithReprocessFailedTasks(TimeSpans.OneDay, 3)
                        .WithMaximumBlocksToGenerate(8));

                    Assert.Equal(8, dateBlocks.Count());
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == DateTimeHelper.CreateUtcDate(2016, 1, 1));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == DateTimeHelper.CreateUtcDate(2016, 1, 2));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == DateTimeHelper.CreateUtcDate(2016, 1, 3));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == DateTimeHelper.CreateUtcDate(2016, 1, 4));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == DateTimeHelper.CreateUtcDate(2016, 1, 5));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == DateTimeHelper.CreateUtcDate(2016, 1, 6));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == DateTimeHelper.CreateUtcDate(2016, 1, 7));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == DateTimeHelper.CreateUtcDate(2016, 1, 8));
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsDateRangeWithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var blockCountLimit = 10;
            await CreateFailedDateTaskAsync();
            await CreateDeadDateTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing(blockCountLimit))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var from = DateTimeHelper.CreateUtcDate(2016, 1, 7);
                    var to = DateTimeHelper.CreateUtcDate(2016, 1, 31);
                    var maxBlockSize = TimeSpans.OneDay;
                    var numericBlocks =
                        await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize));
                    Assert.Equal(10, numericBlocks.Count());
                    Assert.True(numericBlocks.All(x => x.DateRangeBlock.StartDate >= from));
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsNumericRangeWithOverridenConfiguration_ThenOverridenValuesAreUsed()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            await CreateFailedNumericTaskAsync();
            await CreateDeadNumericTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    long from = 61;
                    long to = 200;
                    var maxBlockSize = 10;
                    var numericBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x
                        .WithRange(from, to, maxBlockSize)
                        .OverrideConfiguration()
                        .WithReprocessDeadTasks(TimeSpans.OneDay, 3)
                        .WithReprocessFailedTasks(TimeSpans.OneDay, 3)
                        .WithMaximumBlocksToGenerate(8));

                    Assert.Equal(8, numericBlocks.Count());
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 1);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 11);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 21);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 31);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 41);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 51);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 61);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 71);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsNumericRangeWithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var blockCountLimit = 10;
            await CreateFailedNumericTaskAsync();
            await CreateDeadNumericTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing(blockCountLimit))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    long from = 61;
                    long to = 200;
                    var maxBlockSize = 10;
                    var numericBlocks =
                        await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize));
                    Assert.Equal(10, numericBlocks.Count());
                    Assert.True(numericBlocks.All(x => (int)x.NumericRangeBlock.StartNumber >= 61));
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsDateRange_ForcedBlock_ThenBlockGetsReprocessedAndDequeued()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var fromDate = DateTime.UtcNow.AddHours(-12);
            var toDate = DateTime.UtcNow;

            // create a block
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var maxBlockRange = TimeSpans.OneDay;
                    var rangeBlocks =
                        await executionContext.GetDateRangeBlocksAsync(
                            x => x.WithRange(fromDate, toDate, maxBlockRange));
                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            // add this processed block to the forced queue
            var lastBlockId = _blocksHelper.GetLastBlockId(CurrentTaskId);
            _blocksHelper.EnqueueForcedBlock(lastBlockId, CurrentTaskId);

            // ACT - reprocess the forced block
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithOnlyOldDateBlocks());
                    Assert.Equal(1, rangeBlocks.Count);
                    Assert.Equal(fromDate.ToString("yyyyMMdd HH:mm:ss"),
                        rangeBlocks[0].DateRangeBlock.StartDate.ToString("yyyyMMdd HH:mm:ss"));
                    Assert.Equal(toDate.ToString("yyyyMMdd HH:mm:ss"),
                        rangeBlocks[0].DateRangeBlock.EndDate.ToString("yyyyMMdd HH:mm:ss"));
                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            // The forced block will have been dequeued so it should not be processed again
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithOnlyOldDateBlocks());
                    Assert.Equal(0, rangeBlocks.Count);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsNumericRange_ForcedBlock_ThenBlockGetsReprocessedAndDequeued()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            long fromNumber = 1000;
            long toNumber = 2000;

            // create a block
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var maxBlockRange = 2000;
                    var rangeBlocks =
                        await executionContext.GetNumericRangeBlocksAsync(x =>
                            x.WithRange(fromNumber, toNumber, maxBlockRange));
                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            // add this processed block to the forced queue
            var lastBlockId = _blocksHelper.GetLastBlockId(CurrentTaskId);
            _blocksHelper.EnqueueForcedBlock(lastBlockId, CurrentTaskId);

            // ACT - reprocess the forced block
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithOnlyOldNumericBlocks());
                    Assert.Equal(1, rangeBlocks.Count);
                    Assert.Equal(fromNumber, rangeBlocks[0].NumericRangeBlock.StartNumber);
                    Assert.Equal(toNumber, rangeBlocks[0].NumericRangeBlock.EndNumber);
                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            // The forced block will have been dequeued so it should not be processed again
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithOnlyOldNumericBlocks());
                    Assert.Equal(0, rangeBlocks.Count);
                }
            }
        });
    }

    private ITaskExecutionContext CreateTaskExecutionContext(int maxBlocksToCreate = 2000)
    {
        return _clientHelper.GetExecutionContext(CurrentTaskId,
            _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(maxBlocksToCreate));
    }

    private ITaskExecutionContext CreateTaskExecutionContextWithNoReprocessing(int maxBlocksToCreate = 2000)
    {
        var defaultTaskConfigurationWithKeepAliveAndNoReprocessing =
            _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndNoReprocessing(maxBlocksToCreate);
        _logger.LogDebug(JsonConvert.SerializeObject(defaultTaskConfigurationWithKeepAliveAndNoReprocessing,
            Formatting.Indented));
        return _clientHelper.GetExecutionContext(CurrentTaskId,
            defaultTaskConfigurationWithKeepAliveAndNoReprocessing);
    }

    private async Task CreateFailedDateTaskAsync()
    {
        using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var from = DateTimeHelper.CreateUtcDate(2016, 1, 1);
                var to = DateTimeHelper.CreateUtcDate(2016, 1, 4);
                var maxBlockSize = TimeSpans.OneDay;
                var dateBlocks =
                    await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize));

                foreach (var block in dateBlocks)
                {
                    await block.StartAsync();
                    await block.FailedAsync();
                }
            }
        }
    }

    private async Task CreateDeadDateTaskAsync()
    {
        using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var from = DateTimeHelper.CreateUtcDate(2016, 1, 4);
                var to = DateTimeHelper.CreateUtcDate(2016, 1, 7);
                var maxBlockSize = TimeSpans.OneDay;
                var dateBlocks =
                    await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize));

                foreach (var block in dateBlocks) await block.StartAsync();
            }
        }

        var executionsHelper = _executionsHelper;
        executionsHelper.SetLastExecutionAsDead(_taskDefinitionId);
    }

    private async Task CreateFailedNumericTaskAsync()
    {
        using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                long from = 1;
                long to = 30;
                var maxBlockSize = 10;
                var numericBlocks =
                    await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize));

                foreach (var block in numericBlocks)
                {
                    await block.StartAsync();
                    await block.FailedAsync();
                }
            }
        }
    }

    private async Task CreateDeadNumericTaskAsync()
    {
        using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                long from = 31;
                long to = 60;
                var maxBlockSize = 10;
                var numericBlocks =
                    await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize));

                foreach (var block in numericBlocks) await block.StartAsync();
            }
        }

        var executionsHelper = _executionsHelper;
        executionsHelper.SetLastExecutionAsDead(_taskDefinitionId);
    }
}

public static class AssertExtensions
{
    public static void SimilarDate(this Assert assert, DateTime d1, DateTime d2)
    {
        // _logger.LogDebug($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {Constants.CheckpointName}");
        Assert.True(d2.Subtract(d2).TotalSeconds < 1);
    }
}