using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.ListBlocks;
using Taskling.Contexts;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.Enums;
using Taskling.InfrastructureContracts.TaskExecution;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Contexts.Given_ListBlockContext;

[Collection(CollectionName)]
public class When_GetListBlocksFromExecutionContext : TestBase
{
    private readonly IBlocksHelper _blocksHelper;
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_GetListBlocksFromExecutionContext> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly long _taskDefinitionId;

    public When_GetListBlocksFromExecutionContext(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper,
        IClientHelper clientHelper, ILogger<When_GetListBlocksFromExecutionContext> logger,
        ITaskRepository taskRepository, ILoggerFactory loggerFactory) : base(executionsHelper)
    {
        _logger = logger;
        _blocksHelper = blocksHelper;
        _clientHelper = clientHelper;

        _loggerFactory = loggerFactory;
        _blocksHelper.DeleteBlocks(CurrentTaskId);
        _executionsHelper = executionsHelper;
        _executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);

        _taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
        _executionsHelper.InsertAvailableExecutionToken(_taskDefinitionId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithSingleUnitCommit_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
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
                    var values = GetPersonList(9);
                    var maxBlockSize = 4;
                    var listBlocks =
                        await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithSingleUnitCommit(values, maxBlockSize));
                    // There should be 3 blocks - 4, 4, 1
                    Assert.Equal(3, _blocksHelper.GetBlockCount(CurrentTaskId));
                    var expectedNotStartedCount = 3;
                    var expectedCompletedCount = 0;

                    // All three should be registered as not started
                    Assert.Equal(expectedNotStartedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.NotStarted));
                    Assert.Equal(0,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.Started));
                    Assert.Equal(expectedCompletedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.Completed));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        expectedNotStartedCount--;

                        // There should be one less NotStarted block and exactly 1 Started block
                        Assert.Equal(expectedNotStartedCount,
                            _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                                BlockExecutionStatusEnum.NotStarted));
                        Assert.Equal(1,
                            _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                                BlockExecutionStatusEnum.Started));

                        var expectedCompletedItems = 0;
                        var expectedPendingItems = (await listBlock.GetItemsAsync(ItemStatusEnum.Pending)).Count();
                        // All items should be Pending and 0 Completed
                        Assert.Equal(expectedPendingItems,
                            _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatusEnum.Pending, CurrentTaskId));
                        Assert.Equal(expectedCompletedItems,
                            _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                ItemStatusEnum.Completed, CurrentTaskId));
                        foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatusEnum.Pending))
                        {
                            // do the processing

                            await itemToProcess.CompleteAsync();

                            // More more should be Completed
                            expectedCompletedItems++;
                            Assert.Equal(expectedCompletedItems,
                                _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                    ItemStatusEnum.Completed, CurrentTaskId));
                        }

                        await listBlock.CompleteAsync();

                        // One more block should be completed
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
    public async Task If_AsListWithSingleUnitCommitAndFailsWithReason_ThenReasonIsPersisted()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            long listBlockId = 0;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    var maxBlockSize = 4;
                    var listBlock =
                        (await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithSingleUnitCommit(values, maxBlockSize))).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    await listBlock.StartAsync();

                    var counter = 0;
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatusEnum.Pending))
                    {
                        await itemToProcess.FailedAsync("Exception");

                        counter++;
                    }

                    await listBlock.CompleteAsync();
                }
            }

            Assert.True(_blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatusEnum.Failed, _loggerFactory, CurrentTaskId)
                .All(x => x.StatusReason == "Exception"));
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_LargeValues_ThenValuesArePersistedAndRetrievedOk()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var values = GetLargePersonList(4);

            // ACT and // ASSERT
            bool startedOk;
            long listBlockId = 0;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var maxBlockSize = 4;
                    var listBlock =
                        (await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithSingleUnitCommit(values, maxBlockSize))).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    await listBlock.StartAsync();

                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatusEnum.Pending))
                        await itemToProcess.FailedAsync("Exception");

                    await listBlock.CompleteAsync();
                }
            }

            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var emptyPersonList = new List<PersonDto>();
                    var maxBlockSize = 4;
                    var listBlock =
                        (await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithSingleUnitCommit(emptyPersonList, maxBlockSize))).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    await listBlock.StartAsync();

                    var itemsToProcess = (await listBlock.GetItemsAsync(ItemStatusEnum.Pending, ItemStatusEnum.Failed))
                        .ToList();
                    for (var i = 0; i < itemsToProcess.Count; i++)
                    {
                        AssertSimilarDates(values[i].DateOfBirth, itemsToProcess[i].Value.DateOfBirth);
                        Assert.Equal(values[i].Id, itemsToProcess[i].Value.Id);
                        Assert.Equal(values[i].Name, itemsToProcess[i].Value.Name);
                    }

                    await listBlock.CompleteAsync();
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithNoValues_ThenCheckpointIsPersistedAndEmptyBlockGenerated()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;

            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = new List<PersonDto>();
                    var maxBlockSize = 4;
                    var listBlock =
                        await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithSingleUnitCommit(values, maxBlockSize));
                    Assert.False(listBlock.Any());
                    var execEvent = _executionsHelper.GetLastEvent(_taskDefinitionId);
                    Assert.Equal(EventTypeEnum.CheckPoint, execEvent.EventType);
                    Assert.Equal("No values for generate the block. Emtpy Block context returned.", execEvent.Message);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithSingleUnitCommitAndStepSet_ThenStepIsPersisted()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            long listBlockId = 0;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    var maxBlockSize = 4;
                    var listBlock =
                        (await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithSingleUnitCommit(values, maxBlockSize))).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    await listBlock.StartAsync();

                    var counter = 0;
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatusEnum.Pending))
                    {
                        itemToProcess.Step = 2;
                        await itemToProcess.FailedAsync("Exception");

                        counter++;
                    }

                    await listBlock.CompleteAsync();
                }
            }

            Assert.True(_blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatusEnum.Failed, _loggerFactory, CurrentTaskId)
                .All(x => x.StatusReason == "Exception" && x.Step == 2));
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithBatchCommitAtEnd_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
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
                    var values = GetPersonList(9);
                    var maxBlockSize = 4;
                    var listBlocks =
                        await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithBatchCommitAtEnd(values, maxBlockSize));
                    // There should be 3 blocks - 4, 4, 1
                    Assert.Equal(3, _blocksHelper.GetBlockCount(CurrentTaskId));
                    var expectedNotStartedCount = 3;
                    var expectedCompletedCount = 0;

                    // All three should be registered as not started
                    Assert.Equal(expectedNotStartedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.NotStarted));
                    Assert.Equal(0,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.Started));
                    Assert.Equal(expectedCompletedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.Completed));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        expectedNotStartedCount--;

                        // There should be one less NotStarted block and exactly 1 Started block
                        Assert.Equal(expectedNotStartedCount,
                            _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                                BlockExecutionStatusEnum.NotStarted));
                        Assert.Equal(1,
                            _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                                BlockExecutionStatusEnum.Started));

                        var expectedPendingItems = (await listBlock.GetItemsAsync(ItemStatusEnum.Pending)).Count();
                        // All items should be Pending and 0 Completed
                        Assert.Equal(expectedPendingItems,
                            _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatusEnum.Pending, CurrentTaskId));
                        Assert.Equal(0,
                            _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                ItemStatusEnum.Completed, CurrentTaskId));
                        foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatusEnum.Pending))
                        {
                            // do the processing

                            await itemToProcess.CompleteAsync();

                            // There should be 0 Completed because we batch commit at the end
                            Assert.Equal(0,
                                _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                    ItemStatusEnum.Completed, CurrentTaskId));
                        }

                        await listBlock.CompleteAsync();

                        // All items should be completed now
                        Assert.Equal((await listBlock.GetItemsAsync(ItemStatusEnum.Completed)).Count(),
                            _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                ItemStatusEnum.Completed, CurrentTaskId));

                        // One more block should be completed
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
    public async Task If_AsListWithPeriodicCommit_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
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
                    var values = GetPersonList(26);
                    var maxBlockSize = 15;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x =>
                        x.WithPeriodicCommit(values, maxBlockSize, BatchSizeEnum.Ten));
                    // There should be 2 blocks - 15, 11
                    Assert.Equal(2, _blocksHelper.GetBlockCount(CurrentTaskId));
                    var expectedNotStartedCount = 2;
                    var expectedCompletedCount = 0;

                    // All three should be registered as not started
                    Assert.Equal(expectedNotStartedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.NotStarted));
                    Assert.Equal(0,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.Started));
                    Assert.Equal(expectedCompletedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                            BlockExecutionStatusEnum.Completed));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        expectedNotStartedCount--;

                        // There should be one less NotStarted block and exactly 1 Started block
                        Assert.Equal(expectedNotStartedCount,
                            _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                                BlockExecutionStatusEnum.NotStarted));
                        Assert.Equal(1,
                            _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                                BlockExecutionStatusEnum.Started));

                        var expectedPendingItems = (await listBlock.GetItemsAsync(ItemStatusEnum.Pending)).Count();
                        var expectedCompletedItems = 0;
                        // All items should be Pending and 0 Completed
                        Assert.Equal(expectedPendingItems,
                            _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatusEnum.Pending, CurrentTaskId));
                        Assert.Equal(expectedCompletedItems,
                            _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                ItemStatusEnum.Completed, CurrentTaskId));
                        var itemsProcessed = 0;
                        var itemsCommitted = 0;
                        foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatusEnum.Pending))
                        {
                            itemsProcessed++;
                            // do the processing

                            await itemToProcess.CompleteAsync();

                            // There should be 0 Completed unless we have reached the batch size 10
                            if (itemsProcessed % 10 == 0)
                            {
                                Assert.Equal(10,
                                    _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                        ItemStatusEnum.Completed, CurrentTaskId));
                                itemsCommitted += 10;
                            }
                            else
                            {
                                Assert.Equal(itemsCommitted,
                                    _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                        ItemStatusEnum.Completed, CurrentTaskId));
                            }
                        }

                        await listBlock.CompleteAsync();

                        // All items should be completed now
                        Assert.Equal(itemsProcessed,
                            _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                ItemStatusEnum.Completed, CurrentTaskId));

                        // One more block should be completed
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
    public async Task If_AsListWithPeriodicCommitAndFailsWithReason_ThenReasonIsPersisted()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            long listBlockId = 0;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(14);
                    var maxBlockSize = 20;
                    var listBlock =
                        (await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithPeriodicCommit(values, maxBlockSize, BatchSizeEnum.Ten))).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    await listBlock.StartAsync();

                    var counter = 0;
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatusEnum.Pending))
                    {
                        await itemToProcess.FailedAsync("Exception");

                        counter++;
                    }

                    await listBlock.CompleteAsync();
                }
            }

            Assert.True(_blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatusEnum.Failed, _loggerFactory, CurrentTaskId)
                .All(x => x.StatusReason == "Exception"));
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithPeriodicCommitAndStepSet_ThenStepIsPersisted()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            long listBlockId = 0;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(14);
                    var maxBlockSize = 20;
                    var listBlock =
                        (await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithPeriodicCommit(values, maxBlockSize, BatchSizeEnum.Ten))).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    await listBlock.StartAsync();

                    var counter = 0;
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatusEnum.Pending))
                    {
                        itemToProcess.Step = 2;
                        await itemToProcess.FailedAsync("Exception");

                        counter++;
                    }

                    await listBlock.CompleteAsync();
                }
            }

            var listBlockItems =
                _blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatusEnum.Failed, _loggerFactory, CurrentTaskId);
            Assert.True(listBlockItems.All(x => x.StatusReason == "Exception" && x.Step == 2));
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_PreviousBlock_ThenLastBlockContainsCorrectItems()
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
                    var values = GetPersonList(26);
                    var maxBlockSize = 15;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x =>
                        x.WithPeriodicCommit(values, maxBlockSize, BatchSizeEnum.Ten));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatusEnum.Pending))
                            await itemToProcess.CompleteAsync();

                        await listBlock.CompleteAsync();
                    }
                }
            }

            var expectedPeople = GetPersonList(11, 15);
            var expectedLastBlock = new ListBlock<PersonDto>(_loggerFactory.CreateLogger<ListBlock<PersonDto>>());
            foreach (var person in expectedPeople)
                expectedLastBlock.Items.Add(
                    new ListBlockItem<PersonDto>(_loggerFactory.CreateLogger<ListBlockItem<PersonDto>>())
                        { Value = person });
            // ACT
            IListBlock<PersonDto> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk) lastBlock = await executionContext.GetLastListBlockAsync<PersonDto>();
            }

            Assert.NotNull(lastBlock);

            // ASSERT
            var expectedItems = await expectedLastBlock.GetItemsAsync();

            var lastBlockItems = await lastBlock.GetItemsAsync();
            Assert.Equal(expectedItems.Count, lastBlockItems.Count);
            Assert.Equal(expectedItems[0].Value.Id, lastBlockItems[0].Value.Id);
            Assert.Equal(expectedItems[1].Value.Id, lastBlockItems[1].Value.Id);
            Assert.Equal(expectedItems[2].Value.Id, lastBlockItems[2].Value.Id);
            Assert.Equal(expectedItems[3].Value.Id, lastBlockItems[3].Value.Id);
            Assert.Equal(expectedItems[4].Value.Id, lastBlockItems[4].Value.Id);
            Assert.Equal(expectedItems[5].Value.Id, lastBlockItems[5].Value.Id);
            Assert.Equal(expectedItems[6].Value.Id, lastBlockItems[6].Value.Id);
            Assert.Equal(expectedItems[7].Value.Id, lastBlockItems[7].Value.Id);
            Assert.Equal(expectedItems[8].Value.Id, lastBlockItems[8].Value.Id);
            Assert.Equal(expectedItems[9].Value.Id, lastBlockItems[9].Value.Id);
            Assert.Equal(expectedItems[10].Value.Id, lastBlockItems[10].Value.Id);
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
            IListBlock<PersonDto> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk) lastBlock = await executionContext.GetLastListBlockAsync<PersonDto>();
            }

            // ASSERT
            Assert.Null(lastBlock);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_PreviousBlockIsPhantom_ThenLastBlockNotThisPhantom()
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
                    var values = GetPersonList(3);
                    var maxBlockSize = 15;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x =>
                        x.WithPeriodicCommit(values, maxBlockSize, BatchSizeEnum.Ten));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatusEnum.Pending))
                            await itemToProcess.CompleteAsync();

                        await listBlock.CompleteAsync();
                    }
                }
            }

            _blocksHelper.InsertPhantomListBlock(CurrentTaskId);

            // ACT
            IListBlock<PersonDto> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk) lastBlock = await executionContext.GetLastListBlockAsync<PersonDto>();
            }

            // ASSERT
            var lastBlockItems = await lastBlock.GetItemsAsync();
            Assert.Equal(3, lastBlockItems.Count);
            Assert.Equal(1, lastBlockItems[0].Value.Id);
            Assert.Equal(2, lastBlockItems[1].Value.Id);
            Assert.Equal(3, lastBlockItems[2].Value.Id);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AnItemFails_ThenCompleteSetsStatusAsFailed()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    var maxBlockSize = 4;
                    var listBlock =
                        (await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithSingleUnitCommit(values, maxBlockSize))).First();

                    await listBlock.StartAsync();

                    var counter = 0;
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatusEnum.Pending))
                    {
                        if (counter == 2)
                            await itemToProcess.FailedAsync("Exception");
                        else
                            await itemToProcess.CompleteAsync();

                        counter++;
                    }

                    await listBlock.CompleteAsync();
                }
            }

            Assert.Equal(1,
                _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                    BlockExecutionStatusEnum.Failed));
            Assert.Equal(0,
                _blocksHelper.GetBlockExecutionCountByStatus(CurrentTaskId,
                    BlockExecutionStatusEnum.Completed));
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        If_ReprocessingSpecificExecutionAndItExistsWithMultipleExecutionsAndOnlyOneFailed_ThenBringBackOnFailedBlockWhenRequested()
    {
        await InSemaphoreAsync(async () =>
        {
            var referenceValue = Guid.NewGuid();
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync(referenceValue);
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    var maxBlockSize = 3;
                    var listBlocks =
                        (await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithPeriodicCommit(values, maxBlockSize, BatchSizeEnum.Fifty))).ToList();

                    // block 0 has one failed item
                    await listBlocks[0].StartAsync();

                    var counter = 0;
                    foreach (var itemToProcess in await listBlocks[0].GetItemsAsync(ItemStatusEnum.Pending))
                    {
                        if (counter == 2)
                            await listBlocks[0].ItemFailedAsync(itemToProcess, "Exception", 1);
                        else
                            await listBlocks[0].ItemCompletedAsync(itemToProcess);

                        counter++;
                    }

                    await listBlocks[0].CompleteAsync();

                    // block 1 succeeds
                    await listBlocks[1].StartAsync();

                    foreach (var itemToProcess in await listBlocks[1].GetItemsAsync(ItemStatusEnum.Pending))
                    {
                        await listBlocks[1].ItemCompletedAsync(itemToProcess);

                        counter++;
                    }

                    await listBlocks[1].CompleteAsync();

                    // block 2 never starts
                }
            }

            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var listBlocksToReprocess = (await executionContext.GetListBlocksAsync<PersonDto>(x => x
                        .WithReprocessPeriodicCommit(BatchSizeEnum.Fifty)
                        .PendingAndFailedBlocks()
                        .OfExecutionWith(referenceValue))).ToList();

                    // one failed and one block never started
                    Assert.Equal(2, listBlocksToReprocess.Count);

                    // the block that failed has one failed item
                    var itemsOfB1 =
                        (await listBlocksToReprocess[0].GetItemsAsync(ItemStatusEnum.Failed, ItemStatusEnum.Pending))
                        .ToList();
                    Assert.Single(itemsOfB1);
                    Assert.Equal("Exception", itemsOfB1[0].StatusReason);
                    var expectedStep = 1;
                    Assert.Equal(expectedStep, itemsOfB1[0].Step);

                    // the block that never executed has 3 pending items
                    var itemsOfB2 = await listBlocksToReprocess[1]
                        .GetItemsAsync(ItemStatusEnum.Failed, ItemStatusEnum.Pending);
                    Assert.Equal(3, itemsOfB2.Count());

                    await listBlocksToReprocess[0].CompleteAsync();
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithOverridenConfiguration_ThenOverridenValuesAreUsed()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            await CreateFailedTaskAsync();
            await CreateDeadTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var values = GetPersonList(8);
                    var maxBlockSize = 4;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x
                        .WithSingleUnitCommit(values, maxBlockSize)
                        .OverrideConfiguration()
                        .WithReprocessFailedTasks(TimeSpans.OneDay, 3)
                        .WithReprocessDeadTasks(TimeSpans.OneDay, 3)
                        .WithMaximumBlocksToGenerate(5));
                    // There should be 5 blocks - 3, 3, 3, 3, 4
                    Assert.Equal(5, _blocksHelper.GetBlockCount(CurrentTaskId));
                    Assert.True((await listBlocks[0].GetItemsAsync()).All(x => x.Status == ItemStatusEnum.Failed));
                    Assert.Equal(3, (await listBlocks[0].GetItemsAsync()).Count());
                    Assert.True((await listBlocks[1].GetItemsAsync()).All(x => x.Status == ItemStatusEnum.Failed));
                    Assert.Equal(3, (await listBlocks[1].GetItemsAsync()).Count());
                    Assert.True((await listBlocks[2].GetItemsAsync()).All(x => x.Status == ItemStatusEnum.Pending));
                    Assert.Equal(3, (await listBlocks[2].GetItemsAsync()).Count());
                    Assert.True((await listBlocks[3].GetItemsAsync()).All(x => x.Status == ItemStatusEnum.Pending));
                    Assert.Equal(3, (await listBlocks[3].GetItemsAsync()).Count());
                    Assert.True((await listBlocks[4].GetItemsAsync()).All(x => x.Status == ItemStatusEnum.Pending));
                    Assert.Equal(4, (await listBlocks[4].GetItemsAsync()).Count());
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            await CreateFailedTaskAsync();
            await CreateDeadTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext =
                   CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var values = GetPersonList(8);
                    var maxBlockSize = 4;
                    var listBlocks =
                        await executionContext
                            .GetListBlocksAsync<PersonDto>(x =>
                                x.WithSingleUnitCommit(values,
                                    maxBlockSize));
                    // There should be 2 blocks - 4, 4
                    Assert.Equal(2, listBlocks.Count);
                    Assert.True(
                        (await listBlocks[0].GetItemsAsync()).All(
                            x => x.Status == ItemStatusEnum.Pending));
                    Assert.Equal(4,
                        (await listBlocks[0].GetItemsAsync())
                        .Count());
                    Assert.True(
                        (await listBlocks[1].GetItemsAsync()).All(
                            x => x.Status == ItemStatusEnum.Pending));
                    Assert.Equal(4,
                        (await listBlocks[1].GetItemsAsync())
                        .Count());
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsList_ThenReturnsBlockInOrderOfBlockId()
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
                    var values = GetPersonList(10);
                    var maxBlockSize = 1;
                    var listBlocks =
                        await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithSingleUnitCommit(values, maxBlockSize));

                    var counter = 0;
                    long lastId = 0;
                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();

                        var currentId = listBlock.Block.ListBlockId;
                        if (counter > 0) Assert.Equal(currentId, lastId + 1);

                        lastId = currentId;

                        await listBlock.CompleteAsync();
                    }
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
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(3);
                    var maxBlockSize = 15;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x =>
                        x.WithPeriodicCommit(values, maxBlockSize, BatchSizeEnum.Ten));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatusEnum.Pending))
                            await listBlock.ItemCompletedAsync(itemToProcess);

                        await listBlock.CompleteAsync();
                    }
                }
            }

            // add this processed block to the forced queue
            var lastBlockId = _blocksHelper.GetLastBlockId(CurrentTaskId);
            _blocksHelper.EnqueueForcedBlock(lastBlockId, CurrentTaskId);

            // ACT - reprocess the forced block
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var listBlocks =
                        await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithBatchCommitAtEnd(new List<PersonDto>(), 10));
                    Assert.Equal(1, listBlocks.Count);

                    var items = (await listBlocks[0].GetItemsAsync()).ToList();
                    Assert.Equal(3, items.Count());
                    Assert.Equal(1, items[0].Value.Id);
                    Assert.Equal(2, items[1].Value.Id);
                    Assert.Equal(3, items[2].Value.Id);
                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();

                        foreach (var item in await listBlock.GetItemsAsync())
                            await listBlock.ItemCompletedAsync(item);

                        await listBlock.CompleteAsync();
                    }
                }
            }

            // The forced block will have been dequeued so it should not be processed again
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var items = new List<PersonDto>();
                    var listBlocks =
                        await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(items, 50));
                    Assert.Equal(0, listBlocks.Count);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_BlockItemsAccessedBeforeGetItemsCalled_ThenItemsAreLoadedOkAnyway()
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
                    var values = GetPersonList(10);
                    var maxBlockSize = 1;
                    var listBlocks =
                        await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithSingleUnitCommit(values, maxBlockSize));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();

                        var itemsToProcess = await listBlock.Block.GetItemsAsync();
                        foreach (var item in itemsToProcess)
                            await item.CompleteAsync();

                        await listBlock.CompleteAsync();
                    }
                }
            }
        });
    }

    private async Task CreateFailedTaskAsync()
    {
        using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var values = GetPersonList(6);
                var maxBlockSize = 3;
                var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x =>
                    x.WithPeriodicCommit(values, maxBlockSize, BatchSizeEnum.Ten));

                foreach (var listBlock in listBlocks)
                {
                    await listBlock.StartAsync();
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatusEnum.Pending))
                        await listBlock.ItemFailedAsync(itemToProcess, "Exception");

                    await listBlock.FailedAsync("Something bad happened");
                }
            }
        }
    }

    private async Task CreateDeadTaskAsync()
    {
        using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var values = GetPersonList(6);
                var maxBlockSize = 3;
                var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x =>
                    x.WithPeriodicCommit(values, maxBlockSize, BatchSizeEnum.Ten));

                foreach (var listBlock in listBlocks) await listBlock.StartAsync();
            }
        }

        var executionsHelper = _executionsHelper;
        executionsHelper.SetLastExecutionAsDead(_taskDefinitionId);
    }

    private ITaskExecutionContext CreateTaskExecutionContext(int maxBlocksToGenerate = 10)
    {
        _logger.LogDebug($"Called with maxblocks={maxBlocksToGenerate}");

        return _clientHelper.GetExecutionContext(CurrentTaskId,
            _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(maxBlocksToGenerate));
    }

    private ITaskExecutionContext CreateTaskExecutionContextWithNoReprocessing()
    {
        return _clientHelper.GetExecutionContext(CurrentTaskId,
            _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndNoReprocessing());
    }

    private List<PersonDto> GetPersonList(int count, int skip = 0)
    {
        var people = new List<PersonDto>
        {
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1980, 1, 1), Id = 1, Name = "Terrence" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1981, 1, 1), Id = 2, Name = "Boris" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1982, 1, 1), Id = 3, Name = "Bob" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1983, 1, 1), Id = 4, Name = "Jane" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1984, 1, 1), Id = 5, Name = "Rachel" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1985, 1, 1), Id = 6, Name = "Sarah" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1986, 1, 1), Id = 7, Name = "Brad" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1987, 1, 1), Id = 8, Name = "Phillip" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1988, 1, 1), Id = 9, Name = "Cory" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1989, 1, 1), Id = 10, Name = "Burt" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1990, 1, 1), Id = 11, Name = "Gladis" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1991, 1, 1), Id = 12, Name = "Ethel" },

            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1992, 1, 1), Id = 13, Name = "Terry" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1993, 1, 1), Id = 14, Name = "Bernie" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1994, 1, 1), Id = 15, Name = "Will" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1995, 1, 1), Id = 16, Name = "Jim" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1996, 1, 1), Id = 17, Name = "Eva" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1997, 1, 1), Id = 18, Name = "Susan" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1998, 1, 1), Id = 19, Name = "Justin" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1999, 1, 1), Id = 20, Name = "Gerry" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(2000, 1, 1), Id = 21, Name = "Fitz" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(2001, 1, 1), Id = 22, Name = "Ellie" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(2002, 1, 1), Id = 23, Name = "Gordon" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(2003, 1, 1), Id = 24, Name = "Gail" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(2004, 1, 1), Id = 25, Name = "Gary" },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(2005, 1, 1), Id = 26, Name = "Gabby" }
        };

        return people.Skip(skip).Take(count).ToList();
    }

    private List<PersonDto> GetLargePersonList(int count, int skip = 0)
    {
        var people = new List<PersonDto>
        {
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1980, 1, 1), Id = 1, Name = GetLongName("Terrence") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1981, 1, 1), Id = 2, Name = GetLongName("Boris") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1982, 1, 1), Id = 3, Name = GetLongName("Bob") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1983, 1, 1), Id = 4, Name = GetLongName("Jane") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1984, 1, 1), Id = 5, Name = GetLongName("Rachel") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1985, 1, 1), Id = 6, Name = GetLongName("Sarah") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1986, 1, 1), Id = 7, Name = GetLongName("Brad") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1987, 1, 1), Id = 8, Name = GetLongName("Phillip") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1988, 1, 1), Id = 9, Name = GetLongName("Cory") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1989, 1, 1), Id = 10, Name = GetLongName("Burt") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1990, 1, 1), Id = 11, Name = GetLongName("Gladis") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1991, 1, 1), Id = 12, Name = GetLongName("Ethel") },

            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1992, 1, 1), Id = 13, Name = GetLongName("Terry") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1993, 1, 1), Id = 14, Name = GetLongName("Bernie") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1994, 1, 1), Id = 15, Name = GetLongName("Will") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1995, 1, 1), Id = 16, Name = GetLongName("Jim") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1996, 1, 1), Id = 17, Name = GetLongName("Eva") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1997, 1, 1), Id = 18, Name = GetLongName("Susan") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1998, 1, 1), Id = 19, Name = GetLongName("Justin") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(1999, 1, 1), Id = 20, Name = GetLongName("Gerry") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(2000, 1, 1), Id = 21, Name = GetLongName("Fitz") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(2001, 1, 1), Id = 22, Name = GetLongName("Ellie") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(2002, 1, 1), Id = 23, Name = GetLongName("Gordon") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(2003, 1, 1), Id = 24, Name = GetLongName("Gail") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(2004, 1, 1), Id = 25, Name = GetLongName("Gary") },
            new() { DateOfBirth = DateTimeHelper.CreateUtcDate(2005, 1, 1), Id = 26, Name = GetLongName("Gabby") }
        };

        return people.Skip(skip).Take(count).ToList();
    }

    public string GetLongName(string name)
    {
        var sb = new StringBuilder();
        for (var i = 0; i < 1000; i++)
            sb.Append(" " + name);

        return sb.ToString();
    }
}