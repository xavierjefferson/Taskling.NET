﻿using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.Enums;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Repositories.Given_RangeBlockRepository;

[Collection(CollectionName)]
public class When_ChangeStatus : TestBase
{
    private readonly IBlocksHelper _blocksHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_ChangeStatus> _logger;
    private readonly IRangeBlockRepository _rangeBlockRepository;

    private readonly long _taskDefinitionId;
    private DateTime _baseDateTime;
    private long _blockExecutionId;
    private long _taskExecution1;

    public When_ChangeStatus(IBlocksHelper blocksHelper, IRangeBlockRepository rangeBlockRepository,
        IExecutionsHelper executionsHelper, ILogger<When_ChangeStatus> logger,
        ITaskRepository taskRepository) : base(executionsHelper)
    {
        _logger = logger;
        _blocksHelper = blocksHelper;

        _rangeBlockRepository = rangeBlockRepository;
        _blocksHelper.DeleteBlocks(CurrentTaskId);
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

    private void InsertDateRangeBlock()
    {
        _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId);

        _baseDateTime = DateTimeHelper.CreateUtcDate(2016, 1, 1);
        var block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-30), DateTime.UtcNow, CurrentTaskId);
        _blockExecutionId = _blocksHelper.InsertBlockExecution(_taskExecution1, block1, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatusEnum.Started, CurrentTaskId);
    }

    private void InsertNumericRangeBlock()
    {
        _taskExecution1 = _executionsHelper.InsertOverrideTaskExecution(_taskDefinitionId);

        _baseDateTime = DateTimeHelper.CreateUtcDate(2016, 1, 1);
        var block1 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 1, 100, DateTime.UtcNow, CurrentTaskId);
        _blockExecutionId = _blocksHelper.InsertBlockExecution(_taskExecution1, block1, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatusEnum.Started, CurrentTaskId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_SetStatusOfDateRangeBlock_ThenItemsCountIsCorrect()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertDateRangeBlock();

            var request = new BlockExecutionChangeStatusRequest(
                CurrentTaskId,
                _taskExecution1,
                BlockTypeEnum.DateRange,
                _blockExecutionId,
                BlockExecutionStatusEnum.Completed);
            request.ItemsProcessed = 10000;
            // ACT
            var sut = CreateSut();
            await sut.ChangeStatusAsync(request);

            var itemCount = _blocksHelper.GetBlockExecutionItemCount(_blockExecutionId, CurrentTaskId);

            // ASSERT
            Assert.Equal(10000, itemCount);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_SetStatusOfNumericRangeBlock_ThenItemsCountIsCorrect()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertNumericRangeBlock();

            var request = new BlockExecutionChangeStatusRequest(
                CurrentTaskId,
                _taskExecution1,
                BlockTypeEnum.NumericRange,
                _blockExecutionId,
                BlockExecutionStatusEnum.Completed);
            request.ItemsProcessed = 10000;
            // ACT
            var sut = CreateSut();
            await sut.ChangeStatusAsync(request);

            var itemCount = _blocksHelper.GetBlockExecutionItemCount(_blockExecutionId, CurrentTaskId);

            // ASSERT
            Assert.Equal(10000, itemCount);
        });
    }
}