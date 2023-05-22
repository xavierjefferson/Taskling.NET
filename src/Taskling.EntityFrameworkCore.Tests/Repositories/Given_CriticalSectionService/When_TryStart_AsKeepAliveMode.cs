using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Tasks;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Repositories.Given_CriticalSectionService;

[Collection(TestConstants.CollectionName)]
public class When_TryStart_AsKeepAliveMode : TestBase
{
    private readonly ICriticalSectionRepository _criticalSectionRepository;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_TryStart_AsKeepAliveMode> _logger;


    public When_TryStart_AsKeepAliveMode(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper,
        IClientHelper clientHelper,
        ILogger<When_TryStart_AsKeepAliveMode> logger, ITaskRepository taskRepository,
        ICriticalSectionRepository criticalSectionRepository) : base(executionsHelper)
    {
        _logger = logger;
        _executionsHelper = executionsHelper;

        _criticalSectionRepository = criticalSectionRepository;


        _executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task If_KeepAliveMode_TokenAvailableAndNothingInQueue_ThenGrant()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            var taskExecutionId = _executionsHelper.InsertKeepAliveTaskExecution(taskDefinitionId);
            _executionsHelper.InsertUnlimitedExecutionToken(taskDefinitionId);

            var request = new StartCriticalSectionRequest(CurrentTaskId,
                taskExecutionId,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
            request.KeepAliveDeathThreshold = new TimeSpan(0, 1, 0);

            // ACT
            var sut = _criticalSectionRepository;
            var response = await sut.StartAsync(request);

            // ASSERT
            Assert.Equal(GrantStatus.Granted, response.GrantStatus);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task If_KeepAliveMode_TokenNotAvailableAndNothingInQueue_ThenAddToQueueAndDeny()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertUnlimitedExecutionToken(taskDefinitionId);

            // Create execution 1 and assign critical section to it
            var taskExecutionId1 = _executionsHelper.InsertKeepAliveTaskExecution(taskDefinitionId);
            _executionsHelper.InsertUnavailableCriticalSectionToken(taskDefinitionId, taskExecutionId1);

            // Create second execution
            var taskExecutionId2 = _executionsHelper.InsertKeepAliveTaskExecution(taskDefinitionId);

            var request = new StartCriticalSectionRequest(CurrentTaskId,
                taskExecutionId2,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
            request.KeepAliveDeathThreshold = new TimeSpan(0, 1, 0);

            // ACT
            var sut = _criticalSectionRepository;
            var response = await sut.StartAsync(request);

            // ASSERT
            var isInQueue = _executionsHelper.GetQueueCount(taskExecutionId2) == 1;
            Assert.True(isInQueue);
            Assert.Equal(GrantStatus.Denied, response.GrantStatus);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task If_KeepAliveMode_TokenNotAvailableAndAlreadyInQueue_ThenDoNotAddToQueueAndDeny()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertUnlimitedExecutionToken(taskDefinitionId);

            // Create execution 1 and assign critical section to it
            var taskExecutionId1 = _executionsHelper.InsertKeepAliveTaskExecution(taskDefinitionId);
            _executionsHelper.InsertUnavailableCriticalSectionToken(taskDefinitionId, taskExecutionId1);

            // Create second execution and insert into queue
            var taskExecutionId2 = _executionsHelper.InsertKeepAliveTaskExecution(taskDefinitionId);
            _executionsHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId2);

            var request = new StartCriticalSectionRequest(CurrentTaskId,
                taskExecutionId2,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
            request.KeepAliveDeathThreshold = new TimeSpan(0, 10, 0);

            // ACT
            var sut = _criticalSectionRepository;
            var response = await sut.StartAsync(request);

            // ASSERT
            var numberOfQueueRecords = _executionsHelper.GetQueueCount(taskExecutionId2);
            Assert.Equal(1, numberOfQueueRecords);
            Assert.Equal(GrantStatus.Denied, response.GrantStatus);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task If_KeepAliveMode_TokenAvailableAndIsFirstInQueue_ThenRemoveFromQueueAndGrant()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertUnlimitedExecutionToken(taskDefinitionId);

            // Create execution 1 and create available critical section token
            var taskExecutionId1 = _executionsHelper.InsertKeepAliveTaskExecution(taskDefinitionId);
            _executionsHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId1);
            _executionsHelper.InsertAvailableCriticalSectionToken(taskDefinitionId, 0);

            var request = new StartCriticalSectionRequest(CurrentTaskId,
                taskExecutionId1,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
            request.KeepAliveDeathThreshold = new TimeSpan(0, 1, 0);

            // ACT
            var sut = _criticalSectionRepository;
            var response = await sut.StartAsync(request);

            // ASSERT
            var numberOfQueueRecords = _executionsHelper.GetQueueCount(taskExecutionId1);
            Assert.Equal(0, numberOfQueueRecords);
            Assert.Equal(GrantStatus.Granted, response.GrantStatus);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task If_KeepAliveMode_TokenAvailableAndIsNotFirstInQueue_ThenDoNotChangeQueueAndDeny()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertUnlimitedExecutionToken(taskDefinitionId);

            // Create execution 1 and add it to the queue
            var taskExecutionId1 = _executionsHelper.InsertKeepAliveTaskExecution(taskDefinitionId);
            _executionsHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId1);

            // Create execution 2 and add it to the queue
            var taskExecutionId2 = _executionsHelper.InsertKeepAliveTaskExecution(taskDefinitionId);
            _executionsHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 2, taskExecutionId2);

            // Create an available critical section token
            _executionsHelper.InsertAvailableCriticalSectionToken(taskDefinitionId, 0);

            var request = new StartCriticalSectionRequest(CurrentTaskId,
                taskExecutionId2,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
            request.KeepAliveDeathThreshold = new TimeSpan(0, 1, 0);

            // ACT
            var sut = _criticalSectionRepository;
            var response = await sut.StartAsync(request);

            // ASSERT
            var numberOfQueueRecords = _executionsHelper.GetQueueCount(taskExecutionId2);
            Assert.Equal(1, numberOfQueueRecords);
            Assert.Equal(GrantStatus.Denied, response.GrantStatus);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task
        If_KeepAliveMode_TokenAvailableAndIsNotFirstInQueueButFirstHasExpiredTimeout_ThenRemoveBothFromQueueAndGrant()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);

            var keepAliveThreshold = new TimeSpan(0, 0, 5);

            // Create execution 1 and add it to the queue
            var taskExecutionId1 =
                _executionsHelper.InsertKeepAliveTaskExecution(taskDefinitionId, new TimeSpan(0, 0, 1),
                    keepAliveThreshold);
            _executionsHelper.InsertUnlimitedExecutionToken(taskDefinitionId);
            _executionsHelper.SetKeepAlive(taskExecutionId1);
            _executionsHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId1);

            Thread.Sleep(6000);

            // Create execution 2 and add it to the queue
            var taskExecutionId2 = _executionsHelper.InsertKeepAliveTaskExecution(taskDefinitionId);
            _executionsHelper.SetKeepAlive(taskExecutionId2);
            _executionsHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 2, taskExecutionId2);

            // Create an available critical section token
            _executionsHelper.InsertAvailableCriticalSectionToken(taskDefinitionId, 0);

            var request = new StartCriticalSectionRequest(CurrentTaskId,
                taskExecutionId2,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
            request.KeepAliveDeathThreshold = keepAliveThreshold;

            // ACT
            var sut = _criticalSectionRepository;
            var response = await sut.StartAsync(request);

            // ASSERT
            var numberOfQueueRecordsForExecution1 = _executionsHelper.GetQueueCount(taskExecutionId1);
            var numberOfQueueRecordsForExecution2 = _executionsHelper.GetQueueCount(taskExecutionId2);
            Assert.Equal(0, numberOfQueueRecordsForExecution1);
            Assert.Equal(0, numberOfQueueRecordsForExecution2);
            Assert.Equal(GrantStatus.Granted, response.GrantStatus);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task
        If_KeepAliveMode_TokenAvailableAndIsNotFirstInQueueButFirstHasCompleted_ThenRemoveBothFromQueueAndGrant()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertUnlimitedExecutionToken(taskDefinitionId);

            // Create execution 1 and add it to the queue
            var taskExecutionId1 = _executionsHelper.InsertKeepAliveTaskExecution(taskDefinitionId);
            _executionsHelper.SetKeepAlive(taskExecutionId1);
            _executionsHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId1);
            _executionsHelper.SetTaskExecutionAsCompleted(taskExecutionId1);

            // Create execution 2 and add it to the queue
            var taskExecutionId2 = _executionsHelper.InsertKeepAliveTaskExecution(taskDefinitionId);
            _executionsHelper.SetKeepAlive(taskExecutionId2);
            _executionsHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 2, taskExecutionId2);

            // Create an available critical section token
            _executionsHelper.InsertAvailableCriticalSectionToken(taskDefinitionId, 0);

            var request = new StartCriticalSectionRequest(CurrentTaskId,
                taskExecutionId2,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
            request.KeepAliveDeathThreshold = new TimeSpan(0, 30, 0);

            // ACT
            var sut = _criticalSectionRepository;
            var response = await sut.StartAsync(request);

            // ASSERT
            var numberOfQueueRecordsForExecution1 = _executionsHelper.GetQueueCount(taskExecutionId1);
            var numberOfQueueRecordsForExecution2 = _executionsHelper.GetQueueCount(taskExecutionId2);
            Assert.Equal(0, numberOfQueueRecordsForExecution1);
            Assert.Equal(0, numberOfQueueRecordsForExecution2);
            Assert.Equal(GrantStatus.Granted, response.GrantStatus);
        });
    }
}