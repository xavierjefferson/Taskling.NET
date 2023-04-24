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

namespace Taskling.SqlServer.Tests.Repositories.Given_ObjectBlockRepository;

public class When_GetLastObjectBlock
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

    public When_GetLastObjectBlock()
    {
        _blocksHelper = new BlocksHelper();
        _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
        _executionHelper = new ExecutionsHelper();
        _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

        _taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        _executionHelper.InsertUnlimitedExecutionToken(_taskDefinitionId);

        TaskRepository.ClearCache();
    }

    private ObjectBlockRepository CreateSut()
    {
        return new ObjectBlockRepository(new TaskRepository());
    }

    private void InsertBlocks()
    {
        _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);

        _baseDateTime = new DateTime(2016, 1, 1);
        _block1 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, DateTime.UtcNow, "Testing1");
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatus.Failed);
        Thread.Sleep(10);
        _block2 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, DateTime.UtcNow, "Testing2");
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, _baseDateTime.AddMinutes(-30),
            _baseDateTime.AddMinutes(-30), _baseDateTime.AddMinutes(-35), BlockExecutionStatus.Started);
        Thread.Sleep(10);
        _block3 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, DateTime.UtcNow, "Testing3");
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, _baseDateTime.AddMinutes(-40),
            _baseDateTime.AddMinutes(-40), _baseDateTime.AddMinutes(-45), BlockExecutionStatus.NotStarted);
        Thread.Sleep(10);
        _block4 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, DateTime.UtcNow, "Testing4");
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, _baseDateTime.AddMinutes(-50),
            _baseDateTime.AddMinutes(-50), _baseDateTime.AddMinutes(-55), BlockExecutionStatus.Completed);
        Thread.Sleep(10);
        _block5 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, DateTime.UtcNow, "Testing5");
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block5, _baseDateTime.AddMinutes(-60),
            _baseDateTime.AddMinutes(-60), _baseDateTime.AddMinutes(-65), BlockExecutionStatus.Started);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task ThenReturnLastCreated()
    {
        // ARRANGE
        InsertBlocks();

        // ACT
        var sut = CreateSut();
        var block = await sut.GetLastObjectBlockAsync<string>(CreateRequest());

        // ASSERT
        Assert.Equal(_block5, block.ObjectBlockId);
        Assert.Equal("Testing5", block.Object);
    }


    private LastBlockRequest CreateRequest()
    {
        var request = new LastBlockRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            BlockType.Object);
        request.LastBlockOrder = LastBlockOrder.LastCreated;

        return request;
    }
}