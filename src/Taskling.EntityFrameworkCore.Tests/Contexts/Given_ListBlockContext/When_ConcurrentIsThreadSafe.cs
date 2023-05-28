using System;
using System.Collections.Async;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.Enums;
using Taskling.InfrastructureContracts.TaskExecution;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Contexts.Given_ListBlockContext;

[Collection(CollectionName)]
public class When_ConcurrentIsThreadSafe : TestBase
{
    private const int ListSize = 10000;
    private readonly IBlocksHelper _blocksHelper;
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_ConcurrentIsThreadSafe> _logger;

    public When_ConcurrentIsThreadSafe(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper,
        IClientHelper clientHelper, ILogger<When_ConcurrentIsThreadSafe> logger, ITaskRepository taskRepository) : base(
        executionsHelper)
    {
        _logger = logger;
        _blocksHelper = blocksHelper;
        _clientHelper = clientHelper;

        _blocksHelper.DeleteBlocks(CurrentTaskId);
        _executionsHelper = executionsHelper;
        _executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);

        var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
        _executionsHelper.InsertUnlimitedExecutionToken(taskDefinitionId);
    }

    [Fact]
    [Trait("Speed", "Slow")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsListWithSingleUnitCommit_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
    {
        await InSemaphoreAsync(async () =>
        {
            _logger.LogDebug("Starting test");
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000)))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetList(ListSize);
                    var maxBlockSize = 1000;
                    var listBlocks =
                        await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithSingleUnitCommit(values, maxBlockSize));
                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        var items = await listBlock.GetItemsAsync(ItemStatusEnum.Failed, ItemStatusEnum.Pending);

                        await items.ParallelForEachAsync(async currentItem =>
                        {
                            await listBlock.ItemCompletedAsync(currentItem);
                        });

                        await listBlock.CompleteAsync();

                        // All items should be completed now
                        Assert.Equal((await listBlock.GetItemsAsync(ItemStatusEnum.Completed)).Count(),
                            _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                ItemStatusEnum.Completed, CurrentTaskId));
                    }
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Slow")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsListWithBatchCommitAtEnd_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
    {
        await InSemaphoreAsync(async () =>
        {
            _logger.LogDebug("Starting test");
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000)))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetList(ListSize);
                    var maxBlockSize = 1000;
                    var listBlocks =
                        await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithBatchCommitAtEnd(values, maxBlockSize));
                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        var items = await listBlock.GetItemsAsync(ItemStatusEnum.Failed, ItemStatusEnum.Pending);
                        await items.ParallelForEachAsync(async currentItem =>
                        {
                            await listBlock.ItemCompletedAsync(currentItem);
                        });

                        await listBlock.CompleteAsync();

                        // All items should be completed now
                        Assert.Equal((await listBlock.GetItemsAsync(ItemStatusEnum.Completed)).Count(),
                            _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                ItemStatusEnum.Completed, CurrentTaskId));
                    }
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Slow")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsListWithPeriodicCommit_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
    {
        await InSemaphoreAsync(async () =>
        {
            _logger.LogDebug("Starting test");
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000)))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetList(ListSize);
                    var maxBlockSize = 1000;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x =>
                        x.WithPeriodicCommit(values, maxBlockSize, BatchSizeEnum.Hundred));
                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        var items = await listBlock.GetItemsAsync(ItemStatusEnum.Failed, ItemStatusEnum.Pending);

                        await items.ParallelForEachAsync(async currentItem =>
                        {
                            await listBlock.ItemCompletedAsync(currentItem);
                        });

                        await listBlock.CompleteAsync();

                        // All items should be completed now
                        var expectedCount = (await listBlock.GetItemsAsync(ItemStatusEnum.Completed)).Count();
                        var actualCount =
                            _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId,
                                ItemStatusEnum.Completed, CurrentTaskId);

                        Assert.Equal(expectedCount, actualCount);
                    }
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Slow")]
    [Trait("Area", "Blocks")]
    public async Task
        If_AsListWithSingleUnitCommit_BlocksProcessedInParallel_BlocksListItemsProcessedSequentially_ThenNoConcurrencyIssues()
    {
        await InSemaphoreAsync(async () =>
        {
            _logger.LogDebug("Starting test");
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000)))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetList(ListSize);
                    var maxBlockSize = 1000;
                    var listBlocks =
                        await executionContext.GetListBlocksAsync<PersonDto>(x =>
                            x.WithSingleUnitCommit(values, maxBlockSize));

                    await listBlocks.ParallelForEachAsync(async currentBlock =>
                    {
                        await currentBlock.StartAsync();

                        foreach (var currentItem in await currentBlock.GetItemsAsync(ItemStatusEnum.Pending))
                            await currentBlock.ItemCompletedAsync(currentItem);
                        ;

                        await currentBlock.CompleteAsync();
                        // All items should be completed now
                        Assert.Equal((await currentBlock.GetItemsAsync(ItemStatusEnum.Completed)).Count(),
                            _blocksHelper.GetListBlockItemCountByStatus(currentBlock.ListBlockId,
                                ItemStatusEnum.Completed, CurrentTaskId));
                    });
                }
            }
        });
    }

    private List<PersonDto> GetList(int count)
    {
        _logger.LogDebug("In GetList");
        var list = new List<PersonDto>();

        for (var i = 0; i < count; i++)
            list.Add(new PersonDto { DateOfBirth = DateTimeHelper.CreateUtcDate(1980, 1, 1), Id = i, Name = "Terrence" + i });

        return list;
    }
}