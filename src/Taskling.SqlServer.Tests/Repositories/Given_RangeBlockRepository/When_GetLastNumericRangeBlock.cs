﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.SqlServer.Tests.Repositories.Given_BlockRepository;
using Xunit;
using Xunit.Abstractions;

namespace Taskling.SqlServer.Tests.Repositories.Given_RangeBlockRepository;

[Collection(TestConstants.CollectionName)]
public class When_GetLastNumericRangeBlock : TestBase
{
    private readonly IBlocksHelper _blocksHelper;
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly IRangeBlockRepository _rangeBlockRepository;

    private readonly int _taskDefinitionId;
    private readonly ITestOutputHelper output;
    private DateTime _baseDateTime;

    private long _block1;
    private long _block2;
    private long _block3;
    private long _block4;
    private long _block5;
    private int _taskExecution1;

    public When_GetLastNumericRangeBlock(ITestOutputHelper output, IBlocksHelper blocksHelper,
        IExecutionsHelper executionsHelper, IClientHelper clientHelper, IRangeBlockRepository rangeBlockRepository,
        ITaskRepository taskRepository) : base(executionsHelper)
    {
        this.output = output;
        _blocksHelper = blocksHelper;
        _clientHelper = clientHelper;
        _blocksHelper.DeleteBlocks(CurrentTaskId.ApplicationName);
        _executionsHelper = executionsHelper;
        _rangeBlockRepository = rangeBlockRepository;
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
        _block1 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 1000, 1100, DateTime.UtcNow);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block1, _baseDateTime.AddMinutes(-20),
            _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatus.Failed);
        Thread.Sleep(10);
        _block2 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 900, 1200, DateTime.UtcNow);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block2, _baseDateTime.AddMinutes(-30),
            _baseDateTime.AddMinutes(-30), _baseDateTime.AddMinutes(-35), BlockExecutionStatus.Started);
        Thread.Sleep(10);
        _block3 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 800, 900, DateTime.UtcNow);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block3, _baseDateTime.AddMinutes(-40),
            _baseDateTime.AddMinutes(-40), _baseDateTime.AddMinutes(-45), BlockExecutionStatus.NotStarted);
        Thread.Sleep(10);
        _block4 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 700, 800, DateTime.UtcNow);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block4, _baseDateTime.AddMinutes(-50),
            _baseDateTime.AddMinutes(-50), _baseDateTime.AddMinutes(-55), BlockExecutionStatus.Completed);
        Thread.Sleep(10);
        _block5 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 600, 700, DateTime.UtcNow);
        _blocksHelper.InsertBlockExecution(_taskExecution1, _block5, _baseDateTime.AddMinutes(-60),
            _baseDateTime.AddMinutes(-60), _baseDateTime.AddMinutes(-65), BlockExecutionStatus.Started);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_OrderByLastCreated_ThenReturnLastCreatedAsync()
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
            Assert.Equal(600, block.RangeBeginAsInt());
            Assert.Equal(700, block.RangeEndAsInt());
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_OrderByMaxFromNumber_ThenReturnBlockWithMaxFromNumber()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertBlocks();

            // ACT
            var sut = CreateSut();
            var block = await sut.GetLastRangeBlockAsync(CreateRequest(LastBlockOrder.MaxRangeStartValue));

            // ASSERT
            Assert.Equal(_block1, block.RangeBlockId);
            Assert.Equal(1000, block.RangeBeginAsInt());
            Assert.Equal(1100, block.RangeEndAsInt());
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "Blocks")]
    public async Task If_OrderByMaxToNumber_ThenReturnBlockWithMaxToNumber()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            InsertBlocks();

            // ACT
            var sut = CreateSut();
            var block = await sut.GetLastRangeBlockAsync(CreateRequest(LastBlockOrder.MaxRangeEndValue));

            // ASSERT
            Assert.Equal(_block2, block.RangeBlockId);
            Assert.Equal(900, block.RangeBeginAsInt());
            Assert.Equal(1200, block.RangeEndAsInt());
        });
    }

    private LastBlockRequest CreateRequest(LastBlockOrder lastBlockOrder)
    {
        var request = new LastBlockRequest(CurrentTaskId,
            BlockType.NumericRange);
        request.LastBlockOrder = lastBlockOrder;

        return request;
    }
}