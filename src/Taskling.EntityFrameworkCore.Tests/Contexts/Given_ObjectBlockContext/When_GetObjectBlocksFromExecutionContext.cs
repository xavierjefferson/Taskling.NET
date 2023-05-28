using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Contexts;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.Enums;
using Taskling.InfrastructureContracts.TaskExecution;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Contexts.Given_ObjectBlockContext;

[Collection(CollectionName)]
public class When_GetObjectBlocksFromExecutionContext : TestBase
{
    private readonly IBlocksHelper _blocksHelper;
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_GetObjectBlocksFromExecutionContext> _logger;
    private readonly long _taskDefinitionId;

    public When_GetObjectBlocksFromExecutionContext(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper,
        IClientHelper clientHelper, ILogger<When_GetObjectBlocksFromExecutionContext> logger,
        ITaskRepository taskRepository) : base(executionsHelper)
    {
        _logger = logger;
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
    public async Task If_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
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
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Testing1"));
                    Assert.Equal(1, _blocksHelper.GetBlockCount(CurrentTaskId));
                    var expectedNotStartedCount = 1;
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
    public async Task If_NoBlockNeeded_ThenEmptyListAndEventPersisted()
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
                    var rangeBlocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithNoNewBlocks());
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
    public async Task If_ComplexObjectStored_ThenRetrievedOk()
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
                    var myObject = new MyComplexClass
                    {
                        Id = 10,
                        Name = "Rupert",
                        DateOfBirth = DateTimeHelper.CreateUtcDate(1955, 1, 1),
                        SomeOtherData = new MyOtherComplexClass
                        {
                            Value = 12.6m,
                            Notes = new List<string> { "hello", "goodbye", null }
                        }
                    };

                    var block =
                        (await executionContext.GetObjectBlocksAsync<MyComplexClass>(x => x.WithObject(myObject)))
                        .First();
                    Assert.Equal(myObject.Id, block.Block.Object.Id);
                    Assert.Equal(myObject.Name, block.Block.Object.Name);
                    AssertSimilarDates(myObject.DateOfBirth, block.Block.Object.DateOfBirth);
                    Assert.Equal(myObject.SomeOtherData.Value, block.Block.Object.SomeOtherData.Value);
                    Assert.Equal(myObject.SomeOtherData.Notes[0], block.Block.Object.SomeOtherData.Notes[0]);
                    Assert.Equal(myObject.SomeOtherData.Notes[1], block.Block.Object.SomeOtherData.Notes[1]);
                    Assert.Null(block.Block.Object.SomeOtherData.Notes[2]);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_LargeComplexObjectStored_ThenRetrievedOk()
    {
        var longList = GetLargeListOfStrings();

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
                    var myObject = new MyComplexClass
                    {
                        Id = 10,
                        Name = "Rupert",
                        DateOfBirth = DateTimeHelper.CreateUtcDate(1955, 1, 1),
                        SomeOtherData = new MyOtherComplexClass
                        {
                            Value = 12.6m,
                            Notes = longList
                        }
                    };

                    var block =
                        (await executionContext.GetObjectBlocksAsync<MyComplexClass>(x => x.WithObject(myObject)))
                        .First();
                    Assert.Equal(myObject.Id, block.Block.Object.Id);
                    Assert.Equal(myObject.Name, block.Block.Object.Name);
                    AssertSimilarDates(myObject.DateOfBirth, block.Block.Object.DateOfBirth);
                    Assert.Equal(myObject.SomeOtherData.Value, block.Block.Object.SomeOtherData.Value);
                    Assert.Equal(myObject.SomeOtherData.Notes.Count, block.Block.Object.SomeOtherData.Notes.Count);

                    for (var i = 0; i < myObject.SomeOtherData.Notes.Count; i++)
                        Assert.Equal(myObject.SomeOtherData.Notes[i], block.Block.Object.SomeOtherData.Notes[i]);
                }
            }
        });
    }

    private List<string> GetLargeListOfStrings()
    {
        var list = new List<string>();

        for (var i = 0; i < 1000; i++)
            list.Add("Long value is " + Guid.NewGuid());

        return list;
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_PreviousBlock_ThenLastBlockHasCorrectObjectValue()
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
                    var blocks =
                        await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject(CurrentTaskId.TaskName));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }

            var expectedLastBlock = new ObjectBlock<string>
            {
                Object = CurrentTaskId.TaskName
            };

            // ACT
            IObjectBlock<string> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk) lastBlock = await executionContext.GetLastObjectBlockAsync<string>();
            }

            // ASSERT
            Assert.Equal(expectedLastBlock.Object, lastBlock.Object);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_NoPreviousBlock_ThenLastBlockIsNull()
    {
        await InSemaphoreAsync(async () =>
        {
// ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            IObjectBlock<string> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk) lastBlock = await executionContext.GetLastObjectBlockAsync<string>();
            }

            // ASSERT
            Assert.Null(lastBlock);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_PreviousBlockIsPhantom_ThenLastBlockIsNotThePhantom()
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
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Testing987"));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }

            _blocksHelper.InsertPhantomObjectBlock(CurrentTaskId);

            // ACT
            IObjectBlock<string> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk) lastBlock = await executionContext.GetLastObjectBlockAsync<string>();
            }

            // ASSERT
            Assert.Equal("Testing987", lastBlock.Object);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackTheFailedBlockWhenRequested()
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
                    var blocks = new List<IObjectBlockContext<string>>();
                    blocks.AddRange(
                        await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object1")));
                    blocks.AddRange(
                        await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object2")));
                    blocks.AddRange(
                        await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object3")));
                    blocks.AddRange(
                        await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object4")));
                    blocks.AddRange(
                        await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object5")));

                    await blocks[0].StartAsync();
                    await blocks[0].CompleteAsync(); // completed
                    await blocks[1].StartAsync();
                    await blocks[1].FailedAsync("Something bad happened"); // failed
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
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.Reprocess()
                        .PendingAndFailedBlocks()
                        .OfExecutionWith(referenceValue));

                    Assert.Equal(3, blocks.Count);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackAllBlocksWhenRequested()
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

                    var blocks = new List<IObjectBlockContext<string>>();
                    blocks.AddRange(
                        await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object1")));
                    blocks.AddRange(
                        await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object2")));
                    blocks.AddRange(
                        await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object3")));
                    blocks.AddRange(
                        await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object4")));
                    blocks.AddRange(
                        await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object5")));

                    await blocks[0].StartAsync();
                    await blocks[0].CompleteAsync(); // completed
                    await blocks[1].StartAsync();
                    await blocks[1].FailedAsync("Something bad happened"); // failed
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
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.Reprocess()
                        .AllBlocks()
                        .OfExecutionWith(referenceValue));

                    Assert.Equal(5, blocks.Count);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_WithPreviousDeadBlocks_ThenReprocessOk()
    {
        await InSemaphoreAsync(async () =>
        {
// ARRANGE
            await CreateFailedObjectBlockTaskAsync();
            await CreateDeadObjectBlockTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("TestingDFG")
                        .OverrideConfiguration()
                        .WithReprocessDeadTasks(TimeSpans.OneDay, 3)
                        .WithReprocessFailedTasks(TimeSpans.OneDay, 3)
                        .WithMaximumBlocksToGenerate(8));

                    var counter = 0;
                    foreach (var block in blocks)
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
            await CreateFailedObjectBlockTaskAsync();
            await CreateDeadObjectBlockTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("TestingYHN")
                        .OverrideConfiguration()
                        .WithReprocessDeadTasks(TimeSpans.OneDay, 3)
                        .WithReprocessFailedTasks(TimeSpans.OneDay, 3)
                        .WithMaximumBlocksToGenerate(8));

                    Assert.Equal(3, blocks.Count());
                    Assert.Contains(blocks, x => x.Block.Object == "Dead Task");
                    Assert.Contains(blocks, x => x.Block.Object == "Failed Task");
                    Assert.Contains(blocks, x => x.Block.Object == "TestingYHN");
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_WithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
    {
        await InSemaphoreAsync(async () =>
        {
// ARRANGE
            await CreateFailedObjectBlockTaskAsync();
            await CreateDeadObjectBlockTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Testing YUI"));
                    Assert.Single(blocks);
                    Assert.True(blocks.First().Block.Object == "Testing YUI");
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_ForcedBlock_ThenBlockGetsReprocessedAndDequeued()
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
                    var blocks =
                        await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Testing Hello"));
                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
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
                    var blocks =
                        await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Testing Goodbye"));
                    Assert.Equal(2, blocks.Count);
                    Assert.Equal("Testing Hello", blocks[0].Block.Object);
                    Assert.Equal("Testing Goodbye", blocks[1].Block.Object);
                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }

            // The forced block will have been dequeued so it should not be processed again
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithNoNewBlocks());
                    Assert.Equal(0, blocks.Count);
                }
            }
        });
    }

    private ITaskExecutionContext CreateTaskExecutionContext()
    {
        return _clientHelper.GetExecutionContext(CurrentTaskId,
            _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing());
    }

    private ITaskExecutionContext CreateTaskExecutionContextWithNoReprocessing()
    {
        return _clientHelper.GetExecutionContext(CurrentTaskId,
            _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndNoReprocessing());
    }

    private async Task CreateFailedObjectBlockTaskAsync()
    {
        using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Failed Task"));

                foreach (var block in blocks)
                {
                    await block.StartAsync();
                    await block.FailedAsync();
                }
            }
        }
    }

    private async Task CreateDeadObjectBlockTaskAsync()
    {
        using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var from = DateTimeHelper.CreateUtcDate(2016, 1, 4);
                var to = DateTimeHelper.CreateUtcDate(2016, 1, 7);
                var maxBlockSize = TimeSpans.OneDay;
                var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Dead Task"));

                foreach (var block in blocks) await block.StartAsync();
            }
        }

        var executionsHelper = _executionsHelper;
        executionsHelper.SetLastExecutionAsDead(_taskDefinitionId);
    }
}