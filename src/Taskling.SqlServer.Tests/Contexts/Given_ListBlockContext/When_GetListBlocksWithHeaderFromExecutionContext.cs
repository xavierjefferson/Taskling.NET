using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Contexts;
using Taskling.Events;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Contexts.Given_ListBlockContext;

[Collection(TestConstants.CollectionName)]
public class When_GetListBlocksWithHeaderFromExecutionContext : TestBase
{
    private readonly IBlocksHelper _blocksHelper;
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_GetListBlocksWithHeaderFromExecutionContext> _logger;
    private readonly int _taskDefinitionId;

    public When_GetListBlocksWithHeaderFromExecutionContext(IBlocksHelper blocksHelper,
        IExecutionsHelper executionsHelper, IClientHelper clientHelper,
        ILogger<When_GetListBlocksWithHeaderFromExecutionContext> logger, ITaskRepository taskRepository)
    {
        _blocksHelper = blocksHelper;
        _clientHelper = clientHelper;
        _logger = logger;
        _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
        _executionsHelper = executionsHelper;
        _executionsHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

        _taskDefinitionId = _executionsHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        _executionsHelper.InsertAvailableExecutionToken(_taskDefinitionId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsListWithHeaderWithSingleUnitCommit_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
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
                var testHeader = GetTestHeader();
                var values = GetPersonList(9);
                var maxBlockSize = 4;
                var listBlocks =
                    await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                        x.WithSingleUnitCommit(values, testHeader, maxBlockSize));
                // There should be 3 blocks - 4, 4, 1
                Assert.Equal(3, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                var expectedNotStartedCount = 3;
                var expectedCompletedCount = 0;

                // All three should be registered as not started
                Assert.Equal(expectedNotStartedCount,
                    _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName,
                        BlockExecutionStatus.NotStarted));
                Assert.Equal(0,
                    _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName,
                        BlockExecutionStatus.Started));
                Assert.Equal(expectedCompletedCount,
                    _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName,
                        BlockExecutionStatus.Completed));

                foreach (var listBlock in listBlocks)
                {
                    await listBlock.StartAsync();
                    expectedNotStartedCount--;

                    Assert.Equal(testHeader.PurchaseCode, listBlock.Block.Header.PurchaseCode);
                    AssertSimilarDates(testHeader.FromDate, listBlock.Block.Header.FromDate);
                    AssertSimilarDates(testHeader.ToDate, listBlock.Block.Header.ToDate);

                    // There should be one less NotStarted block and exactly 1 Started block
                    Assert.Equal(expectedNotStartedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName,
                            TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.Equal(1,
                        _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName,
                            TestConstants.TaskName, BlockExecutionStatus.Started));

                    var expectedCompletedItems = 0;
                    var expectedPendingItems = (await listBlock.GetItemsAsync(ItemStatus.Pending)).Count();
                    // All items should be Pending and 0 Completed
                    Assert.Equal(expectedPendingItems,
                        _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Pending));
                    Assert.Equal(expectedCompletedItems,
                        _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                    {
                        // do the processing

                        await itemToProcess.CompleteAsync();

                        // More more should be Completed
                        expectedCompletedItems++;
                        Assert.Equal(expectedCompletedItems,
                            _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                    }

                    await listBlock.CompleteAsync();

                    // One more block should be completed
                    expectedCompletedCount++;
                    Assert.Equal(expectedCompletedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName,
                            TestConstants.TaskName, BlockExecutionStatus.Completed));
                }
            }
        }
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithHeaderWithSingleUnitCommitAndFailsWithReason_ThenReasonIsPersisted()
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
                var testHeader = GetTestHeader();
                var values = GetPersonList(9);
                var maxBlockSize = 4;
                var listBlock =
                    (await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                        x.WithSingleUnitCommit(values, testHeader, maxBlockSize))).First();
                listBlockId = listBlock.Block.ListBlockId;
                await listBlock.StartAsync();

                var counter = 0;
                foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                {
                    await itemToProcess.FailedAsync("Exception");

                    counter++;
                }

                await listBlock.CompleteAsync();
            }
        }

        Assert.True(_blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed)
            .All(x => x.StatusReason == "Exception"));
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_LargeValuesWithLargeHeader_ThenValuesArePersistedAndRetrievedOk()
    {
        // ARRANGE
        var values = GetLargePersonList(4);
        var largeTestHeader = GetLargeTestHeader();

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
                    (await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                        x.WithSingleUnitCommit(values, largeTestHeader, maxBlockSize))).First();
                listBlockId = listBlock.Block.ListBlockId;
                await listBlock.StartAsync();

                foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                    await itemToProcess.FailedAsync("Exception");

                await listBlock.CompleteAsync();
            }
        }

        using (var executionContext = CreateTaskExecutionContext(1))
        {
            startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var testHeader = GetTestHeader();
                var emptyPersonList = new List<PersonDto>();
                var maxBlockSize = 4;
                var listBlock = (await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                    x.WithSingleUnitCommit(emptyPersonList, testHeader, maxBlockSize))).First();
                listBlockId = listBlock.Block.ListBlockId;
                await listBlock.StartAsync();

                Assert.Equal(largeTestHeader.PurchaseCode, listBlock.Block.Header.PurchaseCode);
                AssertSimilarDates(largeTestHeader.FromDate, listBlock.Block.Header.FromDate);
                AssertSimilarDates(largeTestHeader.ToDate, listBlock.Block.Header.ToDate);

                var itemsToProcess = (await listBlock.GetItemsAsync(ItemStatus.Pending, ItemStatus.Failed)).ToList();
                for (var i = 0; i < itemsToProcess.Count; i++)
                {
                    AssertSimilarDates(values[i].DateOfBirth, itemsToProcess[i].Value.DateOfBirth);
                    Assert.Equal(values[i].Id, itemsToProcess[i].Value.Id);
                    Assert.Equal(values[i].Name, itemsToProcess[i].Value.Name);
                }

                await listBlock.CompleteAsync();
            }
        }
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithHeaderWithNoValues_ThenCheckpointIsPersistedAndEmptyBlockGenerated()
    {
        // ARRANGE

        // ACT and // ASSERT
        bool startedOk;

        using (var executionContext = CreateTaskExecutionContext(1))
        {
            startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var testHeader = GetTestHeader();
                var values = new List<PersonDto>();
                var maxBlockSize = 4;
                var listBlock =
                    await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                        x.WithSingleUnitCommit(values, testHeader, maxBlockSize));
                Assert.False(listBlock.Any());
                var execEvent = _executionsHelper.GetLastEvent(_taskDefinitionId);
                Assert.Equal(EventType.CheckPoint, execEvent.EventType);
                Assert.Equal("No values for generate the block. Emtpy Block context returned.", execEvent.Message);
            }
        }
    }


    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithHeaderWithSingleUnitCommitAndStepSet_ThenStepIsPersisted()
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
                var testHeader = GetTestHeader();
                var values = GetPersonList(9);
                var maxBlockSize = 4;
                var listBlock =
                    (await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                        x.WithSingleUnitCommit(values, testHeader, maxBlockSize))).First();
                listBlockId = listBlock.Block.ListBlockId;
                await listBlock.StartAsync();

                var counter = 0;
                foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                {
                    itemToProcess.Step = 2;
                    await itemToProcess.FailedAsync("Exception");

                    counter++;
                }

                await listBlock.CompleteAsync();
            }
        }

        Assert.True(_blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed)
            .All(x => x.StatusReason == "Exception" && x.Step == 2));
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsListWithHeaderWithBatchCommitAtEnd_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
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
                var testHeader = GetTestHeader();
                var values = GetPersonList(9);
                var maxBlockSize = 4;
                var listBlocks =
                    await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                        x.WithBatchCommitAtEnd(values, testHeader, maxBlockSize));
                // There should be 3 blocks - 4, 4, 1
                Assert.Equal(3, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                var expectedNotStartedCount = 3;
                var expectedCompletedCount = 0;

                // All three should be registered as not started
                Assert.Equal(expectedNotStartedCount,
                    _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName,
                        BlockExecutionStatus.NotStarted));
                Assert.Equal(0,
                    _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName,
                        BlockExecutionStatus.Started));
                Assert.Equal(expectedCompletedCount,
                    _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName,
                        BlockExecutionStatus.Completed));

                foreach (var listBlock in listBlocks)
                {
                    await listBlock.StartAsync();
                    expectedNotStartedCount--;

                    // There should be one less NotStarted block and exactly 1 Started block
                    Assert.Equal(expectedNotStartedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName,
                            TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.Equal(1,
                        _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName,
                            TestConstants.TaskName, BlockExecutionStatus.Started));

                    var expectedPendingItems = (await listBlock.GetItemsAsync(ItemStatus.Pending)).Count();
                    // All items should be Pending and 0 Completed
                    Assert.Equal(expectedPendingItems,
                        _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Pending));
                    Assert.Equal(0,
                        _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                    {
                        // do the processing

                        await itemToProcess.CompleteAsync();

                        // There should be 0 Completed because we batch commit at the end
                        Assert.Equal(0,
                            _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                    }

                    await listBlock.CompleteAsync();

                    // All items should be completed now
                    Assert.Equal((await listBlock.GetItemsAsync(ItemStatus.Completed)).Count(),
                        _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));

                    // One more block should be completed
                    expectedCompletedCount++;
                    Assert.Equal(expectedCompletedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName,
                            TestConstants.TaskName, BlockExecutionStatus.Completed));
                }
            }
        }
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsListWithHeaderWithPeriodicCommit_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
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
                var testHeader = GetTestHeader();
                var values = GetPersonList(26);
                var maxBlockSize = 15;
                var listBlocks = await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                    x.WithPeriodicCommit(values, testHeader, maxBlockSize, BatchSize.Ten));
                // There should be 2 blocks - 15, 11
                Assert.Equal(2, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                var expectedNotStartedCount = 2;
                var expectedCompletedCount = 0;

                // All three should be registered as not started
                Assert.Equal(expectedNotStartedCount,
                    _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName,
                        BlockExecutionStatus.NotStarted));
                Assert.Equal(0,
                    _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName,
                        BlockExecutionStatus.Started));
                Assert.Equal(expectedCompletedCount,
                    _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName,
                        BlockExecutionStatus.Completed));

                foreach (var listBlock in listBlocks)
                {
                    await listBlock.StartAsync();
                    expectedNotStartedCount--;

                    // There should be one less NotStarted block and exactly 1 Started block
                    Assert.Equal(expectedNotStartedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName,
                            TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.Equal(1,
                        _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName,
                            TestConstants.TaskName, BlockExecutionStatus.Started));

                    var expectedPendingItems = (await listBlock.GetItemsAsync(ItemStatus.Pending)).Count();
                    var expectedCompletedItems = 0;
                    // All items should be Pending and 0 Completed
                    Assert.Equal(expectedPendingItems,
                        _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Pending));
                    Assert.Equal(expectedCompletedItems,
                        _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                    var itemsProcessed = 0;
                    var itemsCommitted = 0;
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                    {
                        itemsProcessed++;
                        // do the processing

                        await itemToProcess.CompleteAsync();

                        // There should be 0 Completed unless we have reached the batch size 10
                        if (itemsProcessed % 10 == 0)
                        {
                            Assert.Equal(10,
                                _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                    ItemStatus.Completed));
                            itemsCommitted += 10;
                        }
                        else
                        {
                            Assert.Equal(itemsCommitted,
                                _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                    ItemStatus.Completed));
                        }
                    }


                    await listBlock.CompleteAsync();

                    // All items should be completed now
                    Assert.Equal(itemsProcessed,
                        _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));

                    // One more block should be completed
                    expectedCompletedCount++;
                    Assert.Equal(expectedCompletedCount,
                        _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName,
                            TestConstants.TaskName, BlockExecutionStatus.Completed));
                }
            }
        }
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithHeaderWithPeriodicCommitAndFailsWithReason_ThenReasonIsPersisted()
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
                var testHeader = GetTestHeader();
                var values = GetPersonList(14);
                var maxBlockSize = 20;
                var listBlock = (await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                    x.WithPeriodicCommit(values, testHeader, maxBlockSize, BatchSize.Ten))).First();
                listBlockId = listBlock.Block.ListBlockId;
                await listBlock.StartAsync();

                var counter = 0;
                foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                {
                    await itemToProcess.FailedAsync("Exception");

                    counter++;
                }

                await listBlock.CompleteAsync();
            }
        }

        Assert.True(_blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed)
            .All(x => x.StatusReason == "Exception"));
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithHeaderWithPeriodicCommitAndStepSet_ThenStepIsPersisted()
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
                var testHeader = GetTestHeader();
                var values = GetPersonList(14);
                var maxBlockSize = 20;
                var listBlock = (await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                    x.WithPeriodicCommit(values, testHeader, maxBlockSize, BatchSize.Ten))).First();
                listBlockId = listBlock.Block.ListBlockId;
                await listBlock.StartAsync();

                var counter = 0;
                foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                {
                    itemToProcess.Step = 2;
                    await itemToProcess.FailedAsync("Exception");

                    counter++;
                }

                await listBlock.CompleteAsync();
            }
        }

        var listBlockItems = _blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed);
        Assert.True(listBlockItems.All(x => x.StatusReason == "Exception" && x.Step == 2));
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_PreviousBlockWithHeader_ThenLastBlockContainsCorrectItems()
    {
        var testHeader = GetTestHeader();
        testHeader.PurchaseCode = "B";

        // ARRANGE
        // Create previous blocks
        using (var executionContext = CreateTaskExecutionContext())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var values = GetPersonList(26);
                var maxBlockSize = 15;
                var listBlocks = await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                    x.WithPeriodicCommit(values, testHeader, maxBlockSize, BatchSize.Ten));

                foreach (var listBlock in listBlocks)
                {
                    await listBlock.StartAsync();
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                        await itemToProcess.CompleteAsync();

                    await listBlock.CompleteAsync();
                }
            }
        }

        var expectedPeople = GetPersonList(11, 15);
        var expectedLastBlock = new ListBlock<PersonDto>();
        foreach (var person in expectedPeople)
            expectedLastBlock.Items.Add(new ListBlockItem<PersonDto> { Value = person });


        // ACT
        IListBlock<PersonDto, TestHeader> lastBlock = null;
        using (var executionContext = CreateTaskExecutionContext())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk) lastBlock = await executionContext.GetLastListBlockAsync<PersonDto, TestHeader>();
        }

        // ASSERT
        var expectedLastBlockItems = await expectedLastBlock.GetItemsAsync();
        var lastBlockItems = await lastBlock.GetItemsAsync();

        Assert.Equal(testHeader.PurchaseCode, lastBlock.Header.PurchaseCode);
        Assert.Equal(expectedLastBlockItems.Count, lastBlockItems.Count);
        Assert.Equal(expectedLastBlockItems[0].Value.Id, lastBlockItems[0].Value.Id);
        Assert.Equal(expectedLastBlockItems[1].Value.Id, lastBlockItems[1].Value.Id);
        Assert.Equal(expectedLastBlockItems[2].Value.Id, lastBlockItems[2].Value.Id);
        Assert.Equal(expectedLastBlockItems[3].Value.Id, lastBlockItems[3].Value.Id);
        Assert.Equal(expectedLastBlockItems[4].Value.Id, lastBlockItems[4].Value.Id);
        Assert.Equal(expectedLastBlockItems[5].Value.Id, lastBlockItems[5].Value.Id);
        Assert.Equal(expectedLastBlockItems[6].Value.Id, lastBlockItems[6].Value.Id);
        Assert.Equal(expectedLastBlockItems[7].Value.Id, lastBlockItems[7].Value.Id);
        Assert.Equal(expectedLastBlockItems[8].Value.Id, lastBlockItems[8].Value.Id);
        Assert.Equal(expectedLastBlockItems[9].Value.Id, lastBlockItems[9].Value.Id);
        Assert.Equal(expectedLastBlockItems[10].Value.Id, lastBlockItems[10].Value.Id);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_NoPreviousBlockWithHeader_ThenLastBlockIsNull()
    {
        // ARRANGE
        // all previous blocks were deleted in TestInitialize

        // ACT
        IListBlock<PersonDto, TestHeader> lastBlock = null;
        using (var executionContext = CreateTaskExecutionContext())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk) lastBlock = await executionContext.GetLastListBlockAsync<PersonDto, TestHeader>();
        }

        // ASSERT
        Assert.Null(lastBlock);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_PreviousBlockIsPhantomWithHeader_ThenLastBlockNotThisPhantom()
    {
        var testHeader = GetTestHeader();

        // ARRANGE
        // Create previous blocks
        using (var executionContext = CreateTaskExecutionContext())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var values = GetPersonList(3);
                var maxBlockSize = 15;
                var listBlocks = await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                    x.WithPeriodicCommit(values, testHeader, maxBlockSize, BatchSize.Ten));

                foreach (var listBlock in listBlocks)
                {
                    await listBlock.StartAsync();
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                        await itemToProcess.CompleteAsync();

                    await listBlock.CompleteAsync();
                }
            }
        }

        _blocksHelper.InsertPhantomListBlock(TestConstants.ApplicationName, TestConstants.TaskName);

        // ACT
        IListBlock<PersonDto, TestHeader> lastBlock = null;
        using (var executionContext = CreateTaskExecutionContext())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk) lastBlock = await executionContext.GetLastListBlockAsync<PersonDto, TestHeader>();
        }

        // ASSERT
        var lastBlockItems = await lastBlock.GetItemsAsync();

        Assert.Equal(testHeader.PurchaseCode, lastBlock.Header.PurchaseCode);
        Assert.Equal(3, lastBlockItems.Count);
        Assert.Equal(1, lastBlockItems[0].Value.Id);
        Assert.Equal(2, lastBlockItems[1].Value.Id);
        Assert.Equal(3, lastBlockItems[2].Value.Id);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_BlockWithHeaderAndItemFails_ThenCompleteSetsStatusAsFailed()
    {
        // ARRANGE

        // ACT and // ASSERT
        bool startedOk;
        using (var executionContext = CreateTaskExecutionContext(1))
        {
            startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var testHeader = GetTestHeader();
                var values = GetPersonList(9);
                var maxBlockSize = 4;
                var listBlock =
                    (await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                        x.WithSingleUnitCommit(values, testHeader, maxBlockSize))).First();

                await listBlock.StartAsync();

                var counter = 0;
                foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
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
            _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName,
                BlockExecutionStatus.Failed));
        Assert.Equal(0,
            _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName,
                BlockExecutionStatus.Completed));
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task
        If_ReprocessingSpecificExecutionWithHeaderAndItExistsWithMultipleExecutionsAndOnlyOneFailed_ThenBringBackOnFailedBlockWhenRequested()
    {
        var referenceValue = Guid.NewGuid().ToString();
        bool startedOk;
        using (var executionContext = CreateTaskExecutionContext())
        {
            startedOk = await executionContext.TryStartAsync(referenceValue);
            if (startedOk)
            {
                var testHeader = GetTestHeader();
                var values = GetPersonList(9);
                var maxBlockSize = 3;
                var listBlocks = await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                    x.WithPeriodicCommit(values, testHeader, maxBlockSize, BatchSize.Fifty));

                // block 0 has one failed item
                await listBlocks[0].StartAsync();

                var counter = 0;
                foreach (var itemToProcess in await listBlocks[0].GetItemsAsync(ItemStatus.Pending))
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

                foreach (var itemToProcess in await listBlocks[1].GetItemsAsync(ItemStatus.Pending))
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
                var listBlocksToReprocess = await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x => x
                    .ReprocessWithPeriodicCommit(BatchSize.Fifty)
                    .PendingAndFailedBlocks()
                    .OfExecutionWith(referenceValue));

                // one failed and one block never started
                Assert.Equal(2, listBlocksToReprocess.Count);

                // the block that failed has one failed item
                var itemsOfB1 = (await listBlocksToReprocess[0].GetItemsAsync(ItemStatus.Failed, ItemStatus.Pending))
                    .ToList();
                Assert.Single(itemsOfB1);
                Assert.Equal("Exception", itemsOfB1[0].StatusReason);
                var expectedStep = 1;
                Assert.Equal(expectedStep, itemsOfB1[0].Step);

                // the block that never executed has 3 pending items
                var itemsOfB2 = await listBlocksToReprocess[1].GetItemsAsync(ItemStatus.Failed, ItemStatus.Pending);
                Assert.Equal(3, itemsOfB2.Count());

                await listBlocksToReprocess[0].CompleteAsync();
            }
        }
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithOverridenConfigurationWithHeader_ThenOverridenValuesAreUsed()
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
                var testHeader = GetTestHeader();
                var values = GetPersonList(8);
                var maxBlockSize = 4;
                var listBlocks = await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x => x
                    .WithSingleUnitCommit(values, testHeader, maxBlockSize)
                    .OverrideConfiguration()
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                    .MaximumBlocksToGenerate(5));
                // There should be 5 blocks - 3, 3, 3, 3, 4
                Assert.Equal(5, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                Assert.True((await listBlocks[0].GetItemsAsync()).All(x => x.Status == ItemStatus.Failed));
                Assert.Equal(3, (await listBlocks[0].GetItemsAsync()).Count());
                Assert.True((await listBlocks[1].GetItemsAsync()).All(x => x.Status == ItemStatus.Failed));
                Assert.Equal(3, (await listBlocks[1].GetItemsAsync()).Count());
                Assert.True((await listBlocks[2].GetItemsAsync()).All(x => x.Status == ItemStatus.Pending));
                Assert.Equal(3, (await listBlocks[2].GetItemsAsync()).Count());
                Assert.True((await listBlocks[3].GetItemsAsync()).All(x => x.Status == ItemStatus.Pending));
                Assert.Equal(3, (await listBlocks[3].GetItemsAsync()).Count());
                Assert.True((await listBlocks[4].GetItemsAsync()).All(x => x.Status == ItemStatus.Pending));
                Assert.Equal(4, (await listBlocks[4].GetItemsAsync()).Count());
            }
        }
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithNoOverridenConfigurationWithHeader_ThenConfigurationValuesAreUsed()
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
                var testHeader = GetTestHeader();
                var values = GetPersonList(8);
                var maxBlockSize = 4;
                var listBlocks =
                    await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                        x.WithSingleUnitCommit(values, testHeader, maxBlockSize));
                // There should be 2 blocks - 4, 4
                Assert.Equal(2, listBlocks.Count);
                Assert.True((await listBlocks[0].GetItemsAsync()).All(x => x.Status == ItemStatus.Pending));
                Assert.Equal(4, (await listBlocks[0].GetItemsAsync()).Count());
                Assert.True((await listBlocks[1].GetItemsAsync()).All(x => x.Status == ItemStatus.Pending));
                Assert.Equal(4, (await listBlocks[1].GetItemsAsync()).Count());
            }
        }
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_AsListWithHeader_ThenReturnsBlockInOrderOfBlockId()
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
                var testHeader = GetTestHeader();
                var values = GetPersonList(10);
                var maxBlockSize = 1;
                var listBlocks =
                    await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                        x.WithSingleUnitCommit(values, testHeader, maxBlockSize));

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
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_ForceBlockWithHeader_ThenBlockGetsReprocessedAndDequeued()
    {
        var forcedBlockTestHeader = GetTestHeader();
        forcedBlockTestHeader.PurchaseCode = "X";

        // ARRANGE
        using (var executionContext = CreateTaskExecutionContext())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var values = GetPersonList(3);
                var maxBlockSize = 15;
                var listBlocks = await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                    x.WithPeriodicCommit(values, forcedBlockTestHeader, maxBlockSize, BatchSize.Ten));

                foreach (var listBlock in listBlocks)
                {
                    await listBlock.StartAsync();
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                        await listBlock.ItemCompletedAsync(itemToProcess);

                    await listBlock.CompleteAsync();
                }
            }
        }

        // add this processed block to the forced queue
        var lastBlockId = _blocksHelper.GetLastBlockId(TestConstants.ApplicationName, TestConstants.TaskName);
        _blocksHelper.EnqueueForcedBlock(lastBlockId);

        // ACT - reprocess the forced block
        using (var executionContext = CreateTaskExecutionContext())
        {
            var startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var testHeader = GetTestHeader();
                var listBlocks = await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                    x.WithBatchCommitAtEnd(new List<PersonDto>(), testHeader, 10));
                Assert.NotEmpty(listBlocks);
                Assert.Equal(forcedBlockTestHeader.PurchaseCode, listBlocks[0].Block.Header.PurchaseCode);
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
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_BlockItemsAccessedBeforeGetItemsCalled_ThenItemsAreLoadedOkAnyway()
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
                var testHeader = GetTestHeader();
                var values = GetPersonList(10);
                var maxBlockSize = 1;
                var listBlocks =
                    await executionContext.GetListBlocksAsync<PersonDto, TestHeader>(x =>
                        x.WithSingleUnitCommit(values, testHeader, maxBlockSize));

                foreach (var listBlock in listBlocks)
                {
                    await listBlock.StartAsync();

                    var itemsToProcess = await listBlock.GetItemsAsync();
                    foreach (var item in itemsToProcess)
                        await item.CompleteAsync();

                    await listBlock.CompleteAsync();
                }
            }
        }
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
                    x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));

                foreach (var listBlock in listBlocks)
                {
                    await listBlock.StartAsync();
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
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
                    x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));

                foreach (var listBlock in listBlocks) await listBlock.StartAsync();
            }
        }

        var executionsHelper = _executionsHelper;
        executionsHelper.SetLastExecutionAsDead(_taskDefinitionId);
    }

    private ITaskExecutionContext CreateTaskExecutionContext(int maxBlocksToGenerate = 10)
    {
        return _clientHelper.GetExecutionContext(TestConstants.TaskName,
            _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(maxBlocksToGenerate));
    }

    private ITaskExecutionContext CreateTaskExecutionContextWithNoReprocessing()
    {
        return _clientHelper.GetExecutionContext(TestConstants.TaskName,
            _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndNoReprocessing());
    }

    private List<PersonDto> GetPersonList(int count, int skip = 0)
    {
        var people = new List<PersonDto>
        {
            new() { DateOfBirth = new DateTime(1980, 1, 1), Id = 1, Name = "Terrence" },
            new() { DateOfBirth = new DateTime(1981, 1, 1), Id = 2, Name = "Boris" },
            new() { DateOfBirth = new DateTime(1982, 1, 1), Id = 3, Name = "Bob" },
            new() { DateOfBirth = new DateTime(1983, 1, 1), Id = 4, Name = "Jane" },
            new() { DateOfBirth = new DateTime(1984, 1, 1), Id = 5, Name = "Rachel" },
            new() { DateOfBirth = new DateTime(1985, 1, 1), Id = 6, Name = "Sarah" },
            new() { DateOfBirth = new DateTime(1986, 1, 1), Id = 7, Name = "Brad" },
            new() { DateOfBirth = new DateTime(1987, 1, 1), Id = 8, Name = "Phillip" },
            new() { DateOfBirth = new DateTime(1988, 1, 1), Id = 9, Name = "Cory" },
            new() { DateOfBirth = new DateTime(1989, 1, 1), Id = 10, Name = "Burt" },
            new() { DateOfBirth = new DateTime(1990, 1, 1), Id = 11, Name = "Gladis" },
            new() { DateOfBirth = new DateTime(1991, 1, 1), Id = 12, Name = "Ethel" },

            new() { DateOfBirth = new DateTime(1992, 1, 1), Id = 13, Name = "Terry" },
            new() { DateOfBirth = new DateTime(1993, 1, 1), Id = 14, Name = "Bernie" },
            new() { DateOfBirth = new DateTime(1994, 1, 1), Id = 15, Name = "Will" },
            new() { DateOfBirth = new DateTime(1995, 1, 1), Id = 16, Name = "Jim" },
            new() { DateOfBirth = new DateTime(1996, 1, 1), Id = 17, Name = "Eva" },
            new() { DateOfBirth = new DateTime(1997, 1, 1), Id = 18, Name = "Susan" },
            new() { DateOfBirth = new DateTime(1998, 1, 1), Id = 19, Name = "Justin" },
            new() { DateOfBirth = new DateTime(1999, 1, 1), Id = 20, Name = "Gerry" },
            new() { DateOfBirth = new DateTime(2000, 1, 1), Id = 21, Name = "Fitz" },
            new() { DateOfBirth = new DateTime(2001, 1, 1), Id = 22, Name = "Ellie" },
            new() { DateOfBirth = new DateTime(2002, 1, 1), Id = 23, Name = "Gordon" },
            new() { DateOfBirth = new DateTime(2003, 1, 1), Id = 24, Name = "Gail" },
            new() { DateOfBirth = new DateTime(2004, 1, 1), Id = 25, Name = "Gary" },
            new() { DateOfBirth = new DateTime(2005, 1, 1), Id = 26, Name = "Gabby" }
        };

        return people.Skip(skip).Take(count).ToList();
    }

    private List<PersonDto> GetLargePersonList(int count, int skip = 0)
    {
        var people = new List<PersonDto>
        {
            new() { DateOfBirth = new DateTime(1980, 1, 1), Id = 1, Name = GetLongName("Terrence") },
            new() { DateOfBirth = new DateTime(1981, 1, 1), Id = 2, Name = GetLongName("Boris") },
            new() { DateOfBirth = new DateTime(1982, 1, 1), Id = 3, Name = GetLongName("Bob") },
            new() { DateOfBirth = new DateTime(1983, 1, 1), Id = 4, Name = GetLongName("Jane") },
            new() { DateOfBirth = new DateTime(1984, 1, 1), Id = 5, Name = GetLongName("Rachel") },
            new() { DateOfBirth = new DateTime(1985, 1, 1), Id = 6, Name = GetLongName("Sarah") },
            new() { DateOfBirth = new DateTime(1986, 1, 1), Id = 7, Name = GetLongName("Brad") },
            new() { DateOfBirth = new DateTime(1987, 1, 1), Id = 8, Name = GetLongName("Phillip") },
            new() { DateOfBirth = new DateTime(1988, 1, 1), Id = 9, Name = GetLongName("Cory") },
            new() { DateOfBirth = new DateTime(1989, 1, 1), Id = 10, Name = GetLongName("Burt") },
            new() { DateOfBirth = new DateTime(1990, 1, 1), Id = 11, Name = GetLongName("Gladis") },
            new() { DateOfBirth = new DateTime(1991, 1, 1), Id = 12, Name = GetLongName("Ethel") },

            new() { DateOfBirth = new DateTime(1992, 1, 1), Id = 13, Name = GetLongName("Terry") },
            new() { DateOfBirth = new DateTime(1993, 1, 1), Id = 14, Name = GetLongName("Bernie") },
            new() { DateOfBirth = new DateTime(1994, 1, 1), Id = 15, Name = GetLongName("Will") },
            new() { DateOfBirth = new DateTime(1995, 1, 1), Id = 16, Name = GetLongName("Jim") },
            new() { DateOfBirth = new DateTime(1996, 1, 1), Id = 17, Name = GetLongName("Eva") },
            new() { DateOfBirth = new DateTime(1997, 1, 1), Id = 18, Name = GetLongName("Susan") },
            new() { DateOfBirth = new DateTime(1998, 1, 1), Id = 19, Name = GetLongName("Justin") },
            new() { DateOfBirth = new DateTime(1999, 1, 1), Id = 20, Name = GetLongName("Gerry") },
            new() { DateOfBirth = new DateTime(2000, 1, 1), Id = 21, Name = GetLongName("Fitz") },
            new() { DateOfBirth = new DateTime(2001, 1, 1), Id = 22, Name = GetLongName("Ellie") },
            new() { DateOfBirth = new DateTime(2002, 1, 1), Id = 23, Name = GetLongName("Gordon") },
            new() { DateOfBirth = new DateTime(2003, 1, 1), Id = 24, Name = GetLongName("Gail") },
            new() { DateOfBirth = new DateTime(2004, 1, 1), Id = 25, Name = GetLongName("Gary") },
            new() { DateOfBirth = new DateTime(2005, 1, 1), Id = 26, Name = GetLongName("Gabby") }
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

    private TestHeader GetTestHeader()
    {
        return new TestHeader
            { PurchaseCode = "A", FromDate = new DateTime(2016, 1, 1), ToDate = new DateTime(2017, 1, 1) };
    }

    private TestHeader GetLargeTestHeader()
    {
        return new TestHeader
        {
            PurchaseCode = GetLongName("PurchaseCodeA"), FromDate = new DateTime(2016, 1, 1),
            ToDate = new DateTime(2017, 1, 1)
        };
    }
}