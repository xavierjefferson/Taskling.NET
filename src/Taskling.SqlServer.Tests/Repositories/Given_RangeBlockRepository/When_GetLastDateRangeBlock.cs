using System;
using System.Threading;
using System.Threading.Tasks;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.Tasks;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Repositories.Given_RangeBlockRepository;
[Collection(Constants.CollectionName)]
public class When_GetLastDateRangeBlock
{
    private DateTime _baseDateTime;

    private long _block1;
    private long _block2;
    private long _block3;
    private long _block4;
    private long _block5;
    private readonly BlocksHelper _blocksHelper;
    private readonly ExecutionsHelper _executionHelper;

    private readonly int _taskDefinitionId;
    private int _taskExecution1;

    public When_GetLastDateRangeBlock()
    {
        _blocksHelper = new BlocksHelper();
        _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
        _executionHelper = new ExecutionsHelper();
        _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

        _taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        _executionHelper.InsertUnlimitedExecutionToken(_taskDefinitionId);

        TaskRepository.ClearCache();
    }

    private RangeBlockRepository CreateSut()
    {
        return new RangeBlockRepository(new TaskRepository());
    }

    private void InsertBlocks()
    {
        _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);

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
        // ARRANGE
        InsertBlocks();

        // ACT
        var sut = CreateSut();
        var block = await sut.GetLastRangeBlockAsync(CreateRequest(LastBlockOrder.LastCreated));

        // ASSERT
        Assert.Equal(_block5, block.RangeBlockId);
        Assert.Equal(_baseDateTime.AddMinutes(-60), block.RangeBeginAsDateTime());
        Assert.Equal(_baseDateTime.AddMinutes(-70), block.RangeEndAsDateTime());
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_OrderByMaxFromDate_ThenReturnBlockWithMaxFromDate()
    {
        // ARRANGE
        InsertBlocks();

        // ACT
        var sut = CreateSut();
        var block = await sut.GetLastRangeBlockAsync(CreateRequest(LastBlockOrder.MaxRangeStartValue));

        // ASSERT
        Assert.Equal(_block2, block.RangeBlockId);
        Assert.Equal(_baseDateTime.AddMinutes(-10), block.RangeBeginAsDateTime());
        Assert.Equal(_baseDateTime.AddMinutes(-40), block.RangeEndAsDateTime());
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_OrderByMaxToDate_ThenReturnBlockWithMaxToDate()
    {
        // ARRANGE
        InsertBlocks();

        // ACT
        var sut = CreateSut();
        var block = await sut.GetLastRangeBlockAsync(CreateRequest(LastBlockOrder.MaxRangeEndValue));

        // ASSERT
        Assert.Equal(_block1, block.RangeBlockId);
        Assert.Equal(_baseDateTime.AddMinutes(-20), block.RangeBeginAsDateTime());
        Assert.Equal(_baseDateTime.AddMinutes(-30), block.RangeEndAsDateTime());
    }

    private LastBlockRequest CreateRequest(LastBlockOrder lastBlockOrder)
    {
        var request = new LastBlockRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            BlockType.DateRange);
        request.LastBlockOrder = lastBlockOrder;

        return request;
    }
}