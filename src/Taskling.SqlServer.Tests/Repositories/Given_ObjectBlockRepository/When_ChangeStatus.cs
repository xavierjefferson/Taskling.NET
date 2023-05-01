﻿using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Repositories.Given_ObjectBlockRepository;

[Collection(TestConstants.CollectionName)]
public class When_ChangeStatus
{
    private readonly IBlocksHelper _blocksHelper;
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_ChangeStatus> _logger;
    private readonly IObjectBlockRepository _objectBlockRepository;

    private readonly int _taskDefinitionId;
    private DateTime _baseDateTime;
    private long _blockExecutionId;
    private int _taskExecution1;

    public When_ChangeStatus(IBlocksHelper blocksHelper, IObjectBlockRepository objectBlockRepository,
        IExecutionsHelper executionsHelper, IClientHelper clientHelper, ILogger<When_ChangeStatus> logger,
        ITaskRepository taskRepository)
    {
        _blocksHelper = blocksHelper;
        _clientHelper = clientHelper;
        _objectBlockRepository = objectBlockRepository;
        _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
        _executionsHelper = executionsHelper;
        _logger = logger;
        _executionsHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

        _taskDefinitionId = _executionsHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        _executionsHelper.InsertUnlimitedExecutionToken(_taskDefinitionId);

        taskRepository.ClearCache();
    }

    private void InsertObjectBlock()
    {
        _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId);

        _baseDateTime = new DateTime(2016, 1, 1);
        var block1 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, DateTime.UtcNow, Guid.NewGuid().ToString());
        _blockExecutionId = _blocksHelper.InsertBlockExecution(_taskExecution1, block1, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatus.Started);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_SetStatusOfObjectBlock_ThenItemsCountIsCorrect()
    {
        // ARRANGE
        InsertObjectBlock();

        var request = new BlockExecutionChangeStatusRequest(
            new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            _taskExecution1,
            BlockType.Object,
            _blockExecutionId,
            BlockExecutionStatus.Completed);
        request.ItemsProcessed = 10000;


        // ACT
        var sut = _objectBlockRepository;
        await sut.ChangeStatusAsync(request);

        var itemCount = _blocksHelper.GetBlockExecutionItemCount(_blockExecutionId);

        // ASSERT
        Assert.Equal(10000, itemCount);
    }
}