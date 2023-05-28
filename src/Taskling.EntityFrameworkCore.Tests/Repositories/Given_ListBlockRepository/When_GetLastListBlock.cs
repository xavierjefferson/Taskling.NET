using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.Enums;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Serialization;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Repositories.Given_ListBlockRepository;

[Collection(CollectionName)]
public class When_GetLastListBlock : TestBase
{
    private readonly IBlocksHelper _blocksHelper;
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly IListBlockRepository _listBlockRepository;
    private readonly ILogger<When_GetLastListBlock> _logger;

    private readonly long _taskDefinitionId;
    private DateTime _baseDateTime;

    private long _block1;
    private long _block2;
    private long _block3;
    private long _block4;
    private long _block5;
    private long _taskExecution1;

    public When_GetLastListBlock(IBlocksHelper blocksHelper, IListBlockRepository listBlockRepository,
        IExecutionsHelper executionsHelper, IClientHelper clientHelper, ILogger<When_GetLastListBlock> logger,
        ITaskRepository taskRepository) : base(executionsHelper)
    {
        _logger = logger;
        _blocksHelper = blocksHelper;
        _clientHelper = clientHelper;
        _listBlockRepository = listBlockRepository;
        _blocksHelper.DeleteBlocks(CurrentTaskId);
        _executionsHelper = executionsHelper;

        _executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);

        _taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
        _executionsHelper.InsertUnlimitedExecutionToken(_taskDefinitionId);

        taskRepository.ClearCache();
    }

    private void InsertBlocks()
    {
        _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId);

        _baseDateTime = DateTimeHelper.CreateUtcDate(2016, 1, 1);
        var dateRange1 = new DateRange { FromDate = _baseDateTime.AddMinutes(-20), ToDate = _baseDateTime };
        _block1 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow, CurrentTaskId,
            JsonGenericSerializer.Serialize(dateRange1));
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatusEnum.Failed, CurrentTaskId);

        Thread.Sleep(10);
        var dateRange2 = new DateRange { FromDate = _baseDateTime.AddMinutes(-30), ToDate = _baseDateTime };
        _block2 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow, CurrentTaskId,
            JsonGenericSerializer.Serialize(dateRange2));
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, _baseDateTime.AddMinutes(-30),
            _baseDateTime.AddMinutes(-30), _baseDateTime.AddMinutes(-35), BlockExecutionStatusEnum.Started, CurrentTaskId);

        Thread.Sleep(10);
        var dateRange3 = new DateRange { FromDate = _baseDateTime.AddMinutes(-40), ToDate = _baseDateTime };
        _block3 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow, CurrentTaskId,
            JsonGenericSerializer.Serialize(dateRange3));
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, _baseDateTime.AddMinutes(-40),
            _baseDateTime.AddMinutes(-40), _baseDateTime.AddMinutes(-45), BlockExecutionStatusEnum.NotStarted, CurrentTaskId);

        Thread.Sleep(10);
        var dateRange4 = new DateRange { FromDate = _baseDateTime.AddMinutes(-50), ToDate = _baseDateTime };
        _block4 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow, CurrentTaskId,
            JsonGenericSerializer.Serialize(dateRange4));
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, _baseDateTime.AddMinutes(-50),
            _baseDateTime.AddMinutes(-50), _baseDateTime.AddMinutes(-55), BlockExecutionStatusEnum.Completed, CurrentTaskId);

        Thread.Sleep(10);
        var dateRange5 = new DateRange { FromDate = _baseDateTime.AddMinutes(-60), ToDate = _baseDateTime };
        _block5 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow, CurrentTaskId,
            JsonGenericSerializer.Serialize(dateRange5));
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block5, _baseDateTime.AddMinutes(-60),
            _baseDateTime.AddMinutes(-60), _baseDateTime.AddMinutes(-65), BlockExecutionStatusEnum.Started, CurrentTaskId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task ThenReturnLastCreated()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertBlocks();

            // ACT
            var sut = _listBlockRepository;
            var block = await sut.GetLastListBlockAsync(CreateRequest());

            // ASSERT
            Assert.Equal(_block5, block.ListBlockId);
            AssertSimilarDates(DateTimeHelper.CreateUtcDate(2016, 1, 1).AddMinutes(-60),
                JsonGenericSerializer.Deserialize<DateRange>(block.Header).FromDate);
            AssertSimilarDates(DateTimeHelper.CreateUtcDate(2016, 1, 1),
                JsonGenericSerializer.Deserialize<DateRange>(block.Header).ToDate);
        });
    }

    private LastBlockRequest CreateRequest()
    {
        var request = new LastBlockRequest(CurrentTaskId,
            BlockTypeEnum.Object);
        request.LastBlockOrder = LastBlockOrderEnum.LastCreated;

        return request;
    }
}