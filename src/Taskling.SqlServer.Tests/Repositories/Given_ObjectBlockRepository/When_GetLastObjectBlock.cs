using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Repositories.Given_ObjectBlockRepository;

[Collection(TestConstants.CollectionName)]
public class When_GetLastObjectBlock : TestBase
{
    private readonly IBlocksHelper _blocksHelper;
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_GetLastObjectBlock> _logger;
    private readonly IObjectBlockRepository _objectBlockRepository;

    private readonly int _taskDefinitionId;
    private DateTime _baseDateTime;

    private long _block1;
    private long _block2;
    private long _block3;
    private long _block4;
    private long _block5;
    private int _taskExecution1;

    public When_GetLastObjectBlock(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper,
        IClientHelper clientHelper, IObjectBlockRepository objectBlockRepository,
        ILogger<When_GetLastObjectBlock> logger, ITaskRepository taskRepository)
        : base(executionsHelper)
    {
        _logger = logger;
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
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
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId);

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
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
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
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var request = new LastBlockRequest(CurrentTaskId,
            BlockType.Object);
        request.LastBlockOrder = LastBlockOrder.LastCreated;

        return request;
    }
}