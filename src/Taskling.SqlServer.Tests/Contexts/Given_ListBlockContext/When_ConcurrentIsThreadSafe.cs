﻿using System;
using System.Collections.Async;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Taskling.Blocks.ListBlocks;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Contexts.Given_ListBlockContext;

public class When_ConcurrentIsThreadSafe
{
    private readonly BlocksHelper _blocksHelper;
    private readonly ExecutionsHelper _executionHelper;

    public When_ConcurrentIsThreadSafe()
    {
        _blocksHelper = new BlocksHelper();
        _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
        _executionHelper = new ExecutionsHelper();
        _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

        var taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        _executionHelper.InsertUnlimitedExecutionToken(taskDefinitionId);
    }

    [Fact]
    [Trait("Speed", "Slow")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsListWithSingleUnitCommit_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
    {
        // ACT and // ASSERT
        bool startedOk;
        using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName,
                   ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000)))
        {
            startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var values = GetList(100000);
                short maxBlockSize = 1000;
                var listBlocks =
                    await executionContext.GetListBlocksAsync<PersonDto>(x =>
                        x.WithSingleUnitCommit(values, maxBlockSize));
                foreach (var listBlock in listBlocks)
                {
                    await listBlock.StartAsync();
                    var items = await listBlock.GetItemsAsync(ItemStatus.Failed, ItemStatus.Pending);
                    await items.ParallelForEachAsync(async currentItem =>
                    {
                        await listBlock.ItemCompleteAsync(currentItem);
                    });

                    await listBlock.CompleteAsync();

                    // All items should be completed now
                    Assert.Equal((await listBlock.GetItemsAsync(ItemStatus.Completed)).Count(),
                        _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                }
            }
        }
    }

    [Fact]
    [Trait("Speed", "Slow")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsListWithBatchCommitAtEnd_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
    {
        // ACT and // ASSERT
        bool startedOk;
        using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName,
                   ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000)))
        {
            startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var values = GetList(100000);
                short maxBlockSize = 1000;
                var listBlocks =
                    await executionContext.GetListBlocksAsync<PersonDto>(x =>
                        x.WithBatchCommitAtEnd(values, maxBlockSize));
                foreach (var listBlock in listBlocks)
                {
                    await listBlock.StartAsync();
                    var items = await listBlock.GetItemsAsync(ItemStatus.Failed, ItemStatus.Pending);
                    await items.ParallelForEachAsync(async currentItem =>
                    {
                        await listBlock.ItemCompleteAsync(currentItem);
                    });

                    await listBlock.CompleteAsync();

                    // All items should be completed now
                    Assert.Equal((await listBlock.GetItemsAsync(ItemStatus.Completed)).Count(),
                        _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                }
            }
        }
    }

    [Fact]
    [Trait("Speed", "Slow")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsListWithPeriodicCommit_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
    {
        // ACT and // ASSERT
        bool startedOk;
        using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName,
                   ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000)))
        {
            startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var values = GetList(100000);
                short maxBlockSize = 1000;
                var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x =>
                    x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Hundred));
                foreach (var listBlock in listBlocks)
                {
                    await listBlock.StartAsync();
                    var items = await listBlock.GetItemsAsync(ItemStatus.Failed, ItemStatus.Pending);

                    await items.ParallelForEachAsync(async currentItem =>
                    {
                        await listBlock.ItemCompleteAsync(currentItem);
                    });

                    await listBlock.CompleteAsync();

                    // All items should be completed now
                    var expectedCount = (await listBlock.GetItemsAsync(ItemStatus.Completed)).Count();
                    var actualCount =
                        _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed);

                    Assert.Equal(expectedCount, actualCount);
                }
            }
        }
    }

    [Fact]
    [Trait("Speed", "Slow")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsListWithSingleUnitCommit_BlocksProcessedInParallel_BlocksListItemsProcessedSequentially_ThenNoConcurrencyIssues()
    {
        // ACT and // ASSERT
        bool startedOk;
        using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName,
                   ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000)))
        {
            startedOk = await executionContext.TryStartAsync();
            if (startedOk)
            {
                var values = GetList(100000);
                short maxBlockSize = 1000;
                var listBlocks =
                    await executionContext.GetListBlocksAsync<PersonDto>(x =>
                        x.WithSingleUnitCommit(values, maxBlockSize));

                await listBlocks.ParallelForEachAsync(async currentBlock =>
                {
                    await currentBlock.StartAsync();

                    foreach (var currentItem in await currentBlock.GetItemsAsync(ItemStatus.Pending))
                        await currentBlock.ItemCompleteAsync(currentItem);
                    ;

                    await currentBlock.CompleteAsync();
                    // All items should be completed now
                    Assert.Equal((await currentBlock.GetItemsAsync(ItemStatus.Completed)).Count(),
                        _blocksHelper.GetListBlockItemCountByStatus(currentBlock.ListBlockId, ItemStatus.Completed));
                });
            }
        }
    }

    private List<PersonDto> GetList(int count)
    {
        var list = new List<PersonDto>();

        for (var i = 0; i < count; i++)
            list.Add(new PersonDto { DateOfBirth = new DateTime(1980, 1, 1), Id = i, Name = "Terrence" + i });

        return list;
    }
}