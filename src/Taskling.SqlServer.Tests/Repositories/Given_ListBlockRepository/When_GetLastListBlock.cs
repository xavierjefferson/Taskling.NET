using System;
using System.Threading;
using System.Threading.Tasks;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.Serialization;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.Tasks;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Repositories.Given_ListBlockRepository;

public class When_GetLastListBlock
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

    public When_GetLastListBlock()
    {
        _blocksHelper = new BlocksHelper();
        _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
        _executionHelper = new ExecutionsHelper();
        _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

        _taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        _executionHelper.InsertUnlimitedExecutionToken(_taskDefinitionId);

        TaskRepository.ClearCache();
    }

    private ListBlockRepository CreateSut()
    {
        return new ListBlockRepository(new TaskRepository());
    }

    private void InsertBlocks()
    {
        _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);

        _baseDateTime = new DateTime(2016, 1, 1);
        var dateRange1 = new DateRange { FromDate = _baseDateTime.AddMinutes(-20), ToDate = _baseDateTime };
        _block1 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow,
            JsonGenericSerializer.Serialize(dateRange1));
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatus.Failed);

        Thread.Sleep(10);
        var dateRange2 = new DateRange { FromDate = _baseDateTime.AddMinutes(-30), ToDate = _baseDateTime };
        _block2 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow,
            JsonGenericSerializer.Serialize(dateRange2));
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, _baseDateTime.AddMinutes(-30),
            _baseDateTime.AddMinutes(-30), _baseDateTime.AddMinutes(-35), BlockExecutionStatus.Started);

        Thread.Sleep(10);
        var dateRange3 = new DateRange { FromDate = _baseDateTime.AddMinutes(-40), ToDate = _baseDateTime };
        _block3 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow,
            JsonGenericSerializer.Serialize(dateRange3));
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, _baseDateTime.AddMinutes(-40),
            _baseDateTime.AddMinutes(-40), _baseDateTime.AddMinutes(-45), BlockExecutionStatus.NotStarted);

        Thread.Sleep(10);
        var dateRange4 = new DateRange { FromDate = _baseDateTime.AddMinutes(-50), ToDate = _baseDateTime };
        _block4 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow,
            JsonGenericSerializer.Serialize(dateRange4));
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, _baseDateTime.AddMinutes(-50),
            _baseDateTime.AddMinutes(-50), _baseDateTime.AddMinutes(-55), BlockExecutionStatus.Completed);

        Thread.Sleep(10);
        var dateRange5 = new DateRange { FromDate = _baseDateTime.AddMinutes(-60), ToDate = _baseDateTime };
        _block5 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow,
            JsonGenericSerializer.Serialize(dateRange5));
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
        var block = await sut.GetLastListBlockAsync(CreateRequest());

        // ASSERT
        Assert.Equal(_block5, block.ListBlockId);
        Assert.Equal(new DateTime(2016, 1, 1).AddMinutes(-60),
            JsonGenericSerializer.Deserialize<DateRange>(block.Header).FromDate);
        Assert.Equal(new DateTime(2016, 1, 1), JsonGenericSerializer.Deserialize<DateRange>(block.Header).ToDate);
    }


    private LastBlockRequest CreateRequest()
    {
        var request = new LastBlockRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            BlockType.Object);
        request.LastBlockOrder = LastBlockOrder.LastCreated;

        return request;
    }
}