using System;
using System.Threading.Tasks;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.Tasks;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Repositories.Given_RangeBlockRepository;

public class When_ChangeStatus
{
    private DateTime _baseDateTime;
    private long _blockExecutionId;
    private readonly BlocksHelper _blocksHelper;
    private readonly ExecutionsHelper _executionHelper;

    private readonly int _taskDefinitionId;
    private int _taskExecution1;

    public When_ChangeStatus()
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

    private void InsertDateRangeBlock()
    {
        _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);

        _baseDateTime = new DateTime(2016, 1, 1);
        var block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-30), DateTime.UtcNow);
        _blockExecutionId = _blocksHelper.InsertBlockExecution(_taskExecution1, block1, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatus.Started);
    }

    private void InsertNumericRangeBlock()
    {
        _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);

        _baseDateTime = new DateTime(2016, 1, 1);
        var block1 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 1, 100, DateTime.UtcNow);
        _blockExecutionId = _blocksHelper.InsertBlockExecution(_taskExecution1, block1, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatus.Started);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_SetStatusOfDateRangeBlock_ThenItemsCountIsCorrect()
    {
        // ARRANGE
        InsertDateRangeBlock();

        var request = new BlockExecutionChangeStatusRequest(
            new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            _taskExecution1,
            BlockType.DateRange,
            _blockExecutionId,
            BlockExecutionStatus.Completed);
        request.ItemsProcessed = 10000;


        // ACT
        var sut = CreateSut();
        await sut.ChangeStatusAsync(request);

        var itemCount = new BlocksHelper().GetBlockExecutionItemCount(_blockExecutionId);

        // ASSERT
        Assert.Equal(10000, itemCount);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_SetStatusOfNumericRangeBlock_ThenItemsCountIsCorrect()
    {
        // ARRANGE
        InsertNumericRangeBlock();

        var request = new BlockExecutionChangeStatusRequest(
            new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            _taskExecution1,
            BlockType.NumericRange,
            _blockExecutionId,
            BlockExecutionStatus.Completed);
        request.ItemsProcessed = 10000;


        // ACT
        var sut = CreateSut();
        await sut.ChangeStatusAsync(request);

        var itemCount = new BlocksHelper().GetBlockExecutionItemCount(_blockExecutionId);

        // ASSERT
        Assert.Equal(10000, itemCount);
    }
}