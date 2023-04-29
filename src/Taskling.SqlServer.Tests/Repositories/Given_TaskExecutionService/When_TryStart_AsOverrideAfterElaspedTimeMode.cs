﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Events;
using Taskling.SqlServer.TaskExecution;
using Taskling.SqlServer.Tasks;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.SqlServer.Tokens;
using Taskling.SqlServer.Tokens.Executions;
using Taskling.Tasks;
using Xunit;

namespace Taskling.SqlServer.Tests.Repositories.Given_TaskExecutionService;
[Collection(Constants.CollectionName)]
public class When_TryStart_AsOverrideAfterElaspedTimeMode
{
    public When_TryStart_AsOverrideAfterElaspedTimeMode()
    {
        var executionHelper = new ExecutionsHelper();
        executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);
    }

    private TaskExecutionRepository CreateSut()
    {
        return new TaskExecutionRepository(new TaskRepository(),
            new ExecutionTokenRepository(new CommonTokenRepository()), new EventsRepository());
    }

    private TaskExecutionStartRequest CreateOverrideStartRequest(int concurrencyLimit = 1)
    {
        return new TaskExecutionStartRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
            TaskDeathMode.Override, concurrencyLimit, 3, 3)
        {
            OverrideThreshold = new TimeSpan(0, 1, 0),
            TasklingVersion = "N/A"
        };
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task If_TimeOverrideMode_ThenReturnsValidDataValues()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

        var startRequest = CreateOverrideStartRequest();

        // ACT
        var sut = CreateSut();
        var response = await sut.StartAsync(startRequest);

        // ASSERT
        Assert.True(response.TaskExecutionId != 0);
        Assert.True(response.StartedAt > DateTime.MinValue);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task If_TimeOverrideMode_OneTaskAndOneTokenAndIsAvailable_ThenIsGranted()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

        var startRequest = CreateOverrideStartRequest();

        // ACT
        var sut = CreateSut();
        var response = await sut.StartAsync(startRequest);

        // ASSERT
        Assert.Equal(GrantStatus.Granted, response.GrantStatus);
        Assert.NotEqual(Guid.Empty, response.ExecutionTokenId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task
        If_TimeOverrideMode_TwoConcurrentTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndDenyTheOther()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

        var firstStartRequest = CreateOverrideStartRequest();
        var secondStartRequest = CreateOverrideStartRequest();

        // ACT
        var sut = CreateSut();
        var firstResponse = await sut.StartAsync(firstStartRequest);
        var secondResponse = await sut.StartAsync(secondStartRequest);

        // ASSERT
        Assert.Equal(GrantStatus.Granted, firstResponse.GrantStatus);
        Assert.Equal(GrantStatus.Denied, secondResponse.GrantStatus);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task
        If_TimeOverrideMode_TwoSequentialTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndThenGrantTheOther()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

        var firstStartRequest = CreateOverrideStartRequest();
        var secondStartRequest = CreateOverrideStartRequest();

        // ACT
        var sut = CreateSut();
        var firstStartResponse = await sut.StartAsync(firstStartRequest);
        var firstCompleteRequest = new TaskExecutionCompleteRequest(
            new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), firstStartResponse.TaskExecutionId,
            firstStartResponse.ExecutionTokenId);
        var firstCompleteResponse = await sut.CompleteAsync(firstCompleteRequest);

        var secondStartResponse = await sut.StartAsync(secondStartRequest);

        // ASSERT
        Assert.Equal(GrantStatus.Granted, firstStartResponse.GrantStatus);
        Assert.Equal(GrantStatus.Granted, secondStartResponse.GrantStatus);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task
        If_TimeOverrideMode_FiveConcurrentTasksAndFourTokensAndAllAreAvailable_ThenIsGrantFirstFourTasksAndDenyTheOther()
    {
        // ARRANGE
        var concurrencyLimit = 4;
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.InsertAvailableExecutionToken(taskDefinitionId, concurrencyLimit);

        var firstStartRequest = CreateOverrideStartRequest(concurrencyLimit);
        var secondStartRequest = CreateOverrideStartRequest(concurrencyLimit);
        var thirdStartRequest = CreateOverrideStartRequest(concurrencyLimit);
        var fourthStartRequest = CreateOverrideStartRequest(concurrencyLimit);
        var fifthStartRequest = CreateOverrideStartRequest(concurrencyLimit);

        // ACT
        var sut = CreateSut();
        var firstResponse = await sut.StartAsync(firstStartRequest);
        var secondResponse = await sut.StartAsync(secondStartRequest);
        var thirdResponse = await sut.StartAsync(thirdStartRequest);
        var fourthResponse = await sut.StartAsync(fourthStartRequest);
        var fifthResponse = await sut.StartAsync(fifthStartRequest);

        // ASSERT
        Assert.Equal(GrantStatus.Granted, firstResponse.GrantStatus);
        Assert.Equal(GrantStatus.Granted, secondResponse.GrantStatus);
        Assert.Equal(GrantStatus.Granted, thirdResponse.GrantStatus);
        Assert.Equal(GrantStatus.Granted, fourthResponse.GrantStatus);
        Assert.Equal(GrantStatus.Denied, fifthResponse.GrantStatus);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public void If_TimeOverrideMode_OneToken_MultipleTaskThreads_ThenNoDeadLocksOccur()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

        // ACT
        var sut = CreateSut();
        var tasks = new List<Task>();
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));
        tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithTimeOverrideModeAsync(sut)));

        Task.WaitAll(tasks.ToArray());

        // ASSERT
    }

    private async Task RequestAndReturnTokenWithTimeOverrideModeAsync(TaskExecutionRepository sut)
    {
        for (var i = 0; i < 100; i++)
        {
            var firstStartRequest = CreateOverrideStartRequest();

            var firstStartResponse = await sut.StartAsync(firstStartRequest);

            if (firstStartResponse.GrantStatus == GrantStatus.Granted)
            {
                var firstCompleteRequest = new TaskExecutionCompleteRequest(
                    new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                    firstStartResponse.TaskExecutionId, firstStartResponse.ExecutionTokenId);
                var firstCompleteResponse = await sut.CompleteAsync(firstCompleteRequest);
            }
        }
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task
        If_TimeOverrideMode_OneTaskAndOneTokenAndIsUnavailableAndGrantedDateHasPassedElapsedTime_ThenIsGranted()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

        var startRequest = CreateOverrideStartRequest();
        startRequest.OverrideThreshold = new TimeSpan(0, 0, 5);
        var secondRequest = CreateOverrideStartRequest();
        secondRequest.OverrideThreshold = new TimeSpan(0, 0, 5);

        // ACT
        var sut = CreateSut();
        var firstResponse = await sut.StartAsync(startRequest);

        Thread.Sleep(6000);

        var secondResponse = await sut.StartAsync(secondRequest);

        // ASSERT
        Assert.Equal(GrantStatus.Granted, firstResponse.GrantStatus);
        Assert.Equal(GrantStatus.Granted, secondResponse.GrantStatus);
        Assert.NotEqual(Guid.Empty, secondResponse.ExecutionTokenId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task
        If_TimeOverrideMode_OneTaskAndOneTokenAndIsUnavailableAndKeepAliveHasNotPassedElapsedTime_ThenIsDenied()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();
        var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

        var startRequest = CreateOverrideStartRequest();
        var secondRequest = CreateOverrideStartRequest();

        // ACT
        var sut = CreateSut();
        var firstResponse = await sut.StartAsync(startRequest);

        Thread.Sleep(5000);

        var secondResponse = await sut.StartAsync(secondRequest);

        // ASSERT
        Assert.Equal(GrantStatus.Granted, firstResponse.GrantStatus);
        Assert.NotEqual(Guid.Empty, firstResponse.ExecutionTokenId);
        Assert.Equal(GrantStatus.Denied, secondResponse.GrantStatus);
        Assert.Equal(Guid.Empty, secondResponse.ExecutionTokenId);
    }
}