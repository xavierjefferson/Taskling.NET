﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Tasks;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.SqlServer.Tokens;
using Taskling.SqlServer.Tokens.CriticalSections;
using Taskling.Tasks;
using Xunit;

namespace Taskling.SqlServer.Tests.Repositories.Given_CriticalSectionService;
[Collection(Constants.CollectionName)]
public class When_TryStart_AsOverrideAfterElaspedTimeMode
{
    public When_TryStart_AsOverrideAfterElaspedTimeMode()
    {
        var executionHelper = new ExecutionsHelper();
        executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);
    }

    private CriticalSectionRepository CreateSut()
    {
        return new CriticalSectionRepository(new TaskRepository(), new CommonTokenRepository());
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task If_OverrideMode_TokenAvailableAndNothingInQueue_ThenGrant()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        var taskExecutionId = executionHelper.InsertOverrideTaskExecution(taskDefinitionId);
        executionHelper.InsertUnlimitedExecutionToken(taskDefinitionId);

        var request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            taskExecutionId,
            TaskDeathMode.Override,
            CriticalSectionType.User);
        request.OverrideThreshold = new TimeSpan(0, 1, 0);

        // ACT
        var sut = CreateSut();
        var response = await sut.StartAsync(request);

        // ASSERT
        Assert.Equal(GrantStatus.Granted, response.GrantStatus);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task If_OverrideMode_TokenNotAvailableAndNothingInQueue_ThenAddToQueueAndDeny()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.InsertUnlimitedExecutionToken(taskDefinitionId);

        // Create execution 1 and assign critical section to it
        var taskExecutionId1 = executionHelper.InsertOverrideTaskExecution(taskDefinitionId);
        executionHelper.InsertUnavailableCriticalSectionToken(taskDefinitionId, taskExecutionId1);

        // Create second execution
        var taskExecutionId2 = executionHelper.InsertOverrideTaskExecution(taskDefinitionId);

        var request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            taskExecutionId2,
            TaskDeathMode.Override,
            CriticalSectionType.User);
        request.OverrideThreshold = new TimeSpan(0, 1, 0);

        // ACT
        var sut = CreateSut();
        var response = await sut.StartAsync(request);

        // ASSERT
        var isInQueue = executionHelper.GetQueueCount(taskExecutionId2) == 1;
        Assert.True(isInQueue);
        Assert.Equal(GrantStatus.Denied, response.GrantStatus);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task If_OverrideMode_TokenNotAvailableAndAlreadyInQueue_ThenDoNotAddToQueueAndDeny()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.InsertUnlimitedExecutionToken(taskDefinitionId);

        // Create execution 1 and assign critical section to it
        var taskExecutionId1 = executionHelper.InsertOverrideTaskExecution(taskDefinitionId);
        executionHelper.InsertUnavailableCriticalSectionToken(taskDefinitionId, taskExecutionId1);

        // Create second execution and insert into queue
        var taskExecutionId2 = executionHelper.InsertOverrideTaskExecution(taskDefinitionId);
        executionHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId2);

        var request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            taskExecutionId2,
            TaskDeathMode.Override,
            CriticalSectionType.User);
        request.OverrideThreshold = new TimeSpan(0, 10, 0);

        // ACT
        var sut = CreateSut();
        var response = await sut.StartAsync(request);

        // ASSERT
        var numberOfQueueRecords = executionHelper.GetQueueCount(taskExecutionId2);
        Assert.Equal(1, numberOfQueueRecords);
        Assert.Equal(GrantStatus.Denied, response.GrantStatus);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task If_OverrideMode_TokenAvailableAndIsFirstInQueue_ThenRemoveFromQueueAndGrant()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.InsertUnlimitedExecutionToken(taskDefinitionId);

        // Create execution 1 and create available critical section token
        var taskExecutionId1 = executionHelper.InsertOverrideTaskExecution(taskDefinitionId);
        executionHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId1);
        executionHelper.InsertAvailableCriticalSectionToken(taskDefinitionId, 0);

        var request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            taskExecutionId1,
            TaskDeathMode.Override,
            CriticalSectionType.User);
        request.OverrideThreshold = new TimeSpan(0, 1, 0);

        // ACT
        var sut = CreateSut();
        var response = await sut.StartAsync(request);

        // ASSERT
        var numberOfQueueRecords = executionHelper.GetQueueCount(taskExecutionId1);
        Assert.Equal(0, numberOfQueueRecords);
        Assert.Equal(GrantStatus.Granted, response.GrantStatus);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task If_OverrideMode_TokenAvailableAndIsNotFirstInQueue_ThenDoNotChangeQueueAndDeny()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.InsertUnlimitedExecutionToken(taskDefinitionId);

        // Create execution 1 and add it to the queue
        var taskExecutionId1 = executionHelper.InsertOverrideTaskExecution(taskDefinitionId);
        executionHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId1);

        // Create execution 2 and add it to the queue
        var taskExecutionId2 = executionHelper.InsertOverrideTaskExecution(taskDefinitionId);
        executionHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 2, taskExecutionId2);

        // Create an available critical section token
        executionHelper.InsertAvailableCriticalSectionToken(taskDefinitionId, 0);

        var request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            taskExecutionId2,
            TaskDeathMode.Override,
            CriticalSectionType.User);
        request.OverrideThreshold = new TimeSpan(0, 1, 0);

        // ACT
        var sut = CreateSut();
        var response = await sut.StartAsync(request);

        // ASSERT
        var numberOfQueueRecords = executionHelper.GetQueueCount(taskExecutionId2);
        Assert.Equal(1, numberOfQueueRecords);
        Assert.Equal(GrantStatus.Denied, response.GrantStatus);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task
        If_OverrideMode_TokenAvailableAndIsNotFirstInQueueButFirstHasExpiredTimeout_ThenRemoveBothFromQueueAndGrant()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);

        var overrideThreshold = new TimeSpan(0, 0, 5);

        // Create execution 1 and add it to the queue
        var taskExecutionId1 = executionHelper.InsertOverrideTaskExecution(taskDefinitionId, overrideThreshold);
        executionHelper.InsertUnlimitedExecutionToken(taskDefinitionId);
        executionHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId1);

        Thread.Sleep(6000);

        // Create execution 2 and add it to the queue
        var taskExecutionId2 = executionHelper.InsertOverrideTaskExecution(taskDefinitionId, overrideThreshold);
        executionHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 2, taskExecutionId2);

        // Create an available critical section token
        executionHelper.InsertAvailableCriticalSectionToken(taskDefinitionId, 0);

        var request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            taskExecutionId2,
            TaskDeathMode.Override,
            CriticalSectionType.User);
        request.OverrideThreshold = overrideThreshold;

        // ACT
        var sut = CreateSut();
        var response = await sut.StartAsync(request);

        // ASSERT
        var numberOfQueueRecordsForExecution1 = executionHelper.GetQueueCount(taskExecutionId1);
        var numberOfQueueRecordsForExecution2 = executionHelper.GetQueueCount(taskExecutionId2);
        Assert.Equal(0, numberOfQueueRecordsForExecution1);
        Assert.Equal(0, numberOfQueueRecordsForExecution2);
        Assert.Equal(GrantStatus.Granted, response.GrantStatus);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "CriticalSectionTokens")]
    public async Task
        If_OverrideMode_TokenAvailableAndIsNotFirstInQueueButFirstHasCompleted_ThenRemoveBothFromQueueAndGrant()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.InsertUnlimitedExecutionToken(taskDefinitionId);

        // Create execution 1 and add it to the queue
        var taskExecutionId1 = executionHelper.InsertOverrideTaskExecution(taskDefinitionId);
        executionHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId1);
        executionHelper.SetTaskExecutionAsCompleted(taskExecutionId1);

        // Create execution 2 and add it to the queue
        var taskExecutionId2 = executionHelper.InsertOverrideTaskExecution(taskDefinitionId);
        executionHelper.InsertIntoCriticalSectionQueue(taskDefinitionId, 2, taskExecutionId2);

        // Create an available critical section token
        executionHelper.InsertAvailableCriticalSectionToken(taskDefinitionId, 0);

        var request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            taskExecutionId2,
            TaskDeathMode.Override,
            CriticalSectionType.User);
        request.OverrideThreshold = new TimeSpan(0, 30, 0);

        // ACT
        var sut = CreateSut();
        var response = await sut.StartAsync(request);

        // ASSERT
        var numberOfQueueRecordsForExecution1 = executionHelper.GetQueueCount(taskExecutionId1);
        var numberOfQueueRecordsForExecution2 = executionHelper.GetQueueCount(taskExecutionId2);
        Assert.Equal(0, numberOfQueueRecordsForExecution1);
        Assert.Equal(0, numberOfQueueRecordsForExecution2);
        Assert.Equal(GrantStatus.Granted, response.GrantStatus);
    }
}