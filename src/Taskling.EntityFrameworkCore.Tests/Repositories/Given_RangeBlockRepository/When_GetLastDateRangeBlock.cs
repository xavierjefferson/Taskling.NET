using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Repositories.Given_RangeBlockRepository;

[Collection(TestConstants.CollectionName)]
public class When_GetLastDateRangeBlock : TestBase
{
    private readonly IBlocksHelper _blocksHelper;
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_GetLastDateRangeBlock> _logger;
    private readonly IRangeBlockRepository _rangeBlockRepository;

    private readonly long _taskDefinitionId;
    private DateTime _baseDateTime;

    private long _block1;
    private long _block2;
    private long _block3;
    private long _block4;
    private long _block5;
    private long _taskExecution1;

    public When_GetLastDateRangeBlock(IBlocksHelper blocksHelper, IRangeBlockRepository rangeBlockRepository,
        IExecutionsHelper executionsHelper, IClientHelper clientHelper, ILogger<When_GetLastDateRangeBlock> logger,
        ITaskRepository taskRepository) : base(executionsHelper)
    {
        _logger = logger;
        _blocksHelper = blocksHelper;
        _clientHelper = clientHelper;

        _rangeBlockRepository = rangeBlockRepository;
        _blocksHelper.DeleteBlocks(CurrentTaskId.ApplicationName);
        _executionsHelper = executionsHelper;
        _executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);

        _taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
        _executionsHelper.InsertUnlimitedExecutionToken(_taskDefinitionId);

        taskRepository.ClearCache();
    }

    private IRangeBlockRepository CreateSut()
    {
        return _rangeBlockRepository;
    }

    private void InsertBlocks()
    {
        _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId);

        _baseDateTime = new DateTime(2016, 1, 1);
        _block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-30), DateTime.UtcNow);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatus.Failed);
        Thread.Sleep(10);
        _block2 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, _baseDateTime.AddMinutes(-10),
            _baseDateTime.AddMinutes(-40), DateTime.UtcNow);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, _baseDateTime.AddMinutes(-30),
            _baseDateTime.AddMinutes(-30), _baseDateTime.AddMinutes(-35), BlockExecutionStatus.Started);
        Thread.Sleep(10);
        _block3 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, _baseDateTime.AddMinutes(-40),
            _baseDateTime.AddMinutes(-50), DateTime.UtcNow);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, _baseDateTime.AddMinutes(-40),
            _baseDateTime.AddMinutes(-40), _baseDateTime.AddMinutes(-45), BlockExecutionStatus.NotStarted);
        Thread.Sleep(10);
        _block4 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, _baseDateTime.AddMinutes(-50),
            _baseDateTime.AddMinutes(-60), DateTime.UtcNow);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, _baseDateTime.AddMinutes(-50),
            _baseDateTime.AddMinutes(-50), _baseDateTime.AddMinutes(-55), BlockExecutionStatus.Completed);
        Thread.Sleep(10);
        _block5 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, _baseDateTime.AddMinutes(-60),
            _baseDateTime.AddMinutes(-70), DateTime.UtcNow);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block5, _baseDateTime.AddMinutes(-60),
            _baseDateTime.AddMinutes(-60), _baseDateTime.AddMinutes(-65), BlockExecutionStatus.Started);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_OrderByLastCreated_ThenReturnLastCreated()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertBlocks();

            // ACT
            var sut = CreateSut();
            var block = await sut.GetLastRangeBlockAsync(CreateRequest(LastBlockOrder.LastCreated));

            // ASSERT
            Assert.Equal(_block5, block.RangeBlockId);
            AssertSimilarDates(_baseDateTime.AddMinutes(-60), block.RangeBeginAsDateTime());
            AssertSimilarDates(_baseDateTime.AddMinutes(-70), block.RangeEndAsDateTime());
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_OrderByMaxFromDate_ThenReturnBlockWithMaxFromDate()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertBlocks();

            // ACT
            var sut = CreateSut();
            var block = await sut.GetLastRangeBlockAsync(CreateRequest(LastBlockOrder.MaxRangeStartValue));

            // ASSERT
            Assert.Equal(_block2, block.RangeBlockId);
            AssertSimilarDates(_baseDateTime.AddMinutes(-10), block.RangeBeginAsDateTime());
            AssertSimilarDates(_baseDateTime.AddMinutes(-40), block.RangeEndAsDateTime());
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_OrderByMaxToDate_ThenReturnBlockWithMaxToDate()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertBlocks();

            // ACT
            var sut = CreateSut();
            var block = await sut.GetLastRangeBlockAsync(CreateRequest(LastBlockOrder.MaxRangeEndValue));

            // ASSERT
            Assert.Equal(_block1, block.RangeBlockId);
            AssertSimilarDates(_baseDateTime.AddMinutes(-20), block.RangeBeginAsDateTime());
            AssertSimilarDates(_baseDateTime.AddMinutes(-30), block.RangeEndAsDateTime());
        });
    }

    private LastBlockRequest CreateRequest(LastBlockOrder lastBlockOrder)
    {
        var request = new LastBlockRequest(CurrentTaskId,
            BlockType.DateRange);
        request.LastBlockOrder = lastBlockOrder;

        return request;
    }
}