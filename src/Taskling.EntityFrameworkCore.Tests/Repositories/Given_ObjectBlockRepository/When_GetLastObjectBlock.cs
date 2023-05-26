using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.Enums;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Repositories.Given_ObjectBlockRepository;

[Collection(TestConstants.CollectionName)]
public class When_GetLastObjectBlock : TestBase
{
    private readonly IBlocksHelper _blocksHelper;
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_GetLastObjectBlock> _logger;
    private readonly IObjectBlockRepository _objectBlockRepository;

    private readonly long _taskDefinitionId;
    private DateTime _baseDateTime;

    private long _block1;
    private long _block2;
    private long _block3;
    private long _block4;
    private long _block5;
    private long _taskExecution1;

    public When_GetLastObjectBlock(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper,
        IClientHelper clientHelper, IObjectBlockRepository objectBlockRepository,
        ILogger<When_GetLastObjectBlock> logger, ITaskRepository taskRepository)
        : base(executionsHelper)
    {
        _logger = logger;
        _blocksHelper = blocksHelper;
        _clientHelper = clientHelper;
        _blocksHelper.DeleteBlocks(CurrentTaskId.ApplicationName);
        _executionsHelper = executionsHelper;
        _objectBlockRepository = objectBlockRepository;

        _executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);

        _taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
        _executionsHelper.InsertUnlimitedExecutionToken(_taskDefinitionId);

        taskRepository.ClearCache();
    }

    private void InsertBlocks()
    {
        _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId);

        _baseDateTime = new DateTime(2016, 1, 1);
        _block1 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, DateTime.UtcNow, "Testing1");
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatusEnum.Failed);
        Thread.Sleep(10);
        _block2 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, DateTime.UtcNow, "Testing2");
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, _baseDateTime.AddMinutes(-30),
            _baseDateTime.AddMinutes(-30), _baseDateTime.AddMinutes(-35), BlockExecutionStatusEnum.Started);
        Thread.Sleep(10);
        _block3 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, DateTime.UtcNow, "Testing3");
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, _baseDateTime.AddMinutes(-40),
            _baseDateTime.AddMinutes(-40), _baseDateTime.AddMinutes(-45), BlockExecutionStatusEnum.NotStarted);
        Thread.Sleep(10);
        _block4 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, DateTime.UtcNow, "Testing4");
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, _baseDateTime.AddMinutes(-50),
            _baseDateTime.AddMinutes(-50), _baseDateTime.AddMinutes(-55), BlockExecutionStatusEnum.Completed);
        Thread.Sleep(10);
        _block5 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, DateTime.UtcNow, "Testing5");
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block5, _baseDateTime.AddMinutes(-60),
            _baseDateTime.AddMinutes(-60), _baseDateTime.AddMinutes(-65), BlockExecutionStatusEnum.Started);
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
            var block = await _objectBlockRepository.GetLastObjectBlockAsync<string>(CreateRequest());

            // ASSERT
            Assert.Equal(_block5, block.ObjectBlockId);
            Assert.Equal("Testing5", block.Object);
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