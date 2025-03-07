﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.EntityFrameworkCore.Tokens.Executions;
using Taskling.Enums;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Repositories.Given_TaskExecutionService;

[Collection(CollectionName)]
public class When_TryStart_AsKeepAliveMode : TestBase
{
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_TryStart_AsKeepAliveMode> _logger;
    private readonly ITaskExecutionRepository _taskExecutionRepository;

    public When_TryStart_AsKeepAliveMode(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper,
        IClientHelper clientHelper, ILogger<When_TryStart_AsKeepAliveMode> logger, ITaskRepository taskRepository,
        ITaskExecutionRepository taskExecutionRepository) : base(executionsHelper)
    {
        _logger = logger;
        _executionsHelper = executionsHelper;
        _clientHelper = clientHelper;

        _taskExecutionRepository = taskExecutionRepository;

        _executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);

        taskRepository.ClearCache();
    }

    private TaskExecutionStartRequest CreateKeepAliveStartRequest(int concurrencyLimit = 1)
    {
        return new TaskExecutionStartRequest(CurrentTaskId,
            TaskDeathModeEnum.KeepAlive, concurrencyLimit, 3, 3)
        {
            KeepAliveDeathThreshold = TimeSpans.OneMinute,
            KeepAliveInterval = TimeSpans.TwentySeconds,
            TasklingVersion = "N/A"
        };
    }

    private SendKeepAliveRequest CreateKeepAliveRequest(TaskId taskId, long taskExecutionId,
        Guid executionTokenId)
    {
        return new SendKeepAliveRequest(taskId)
        {
            TaskExecutionId = taskExecutionId,
            ExecutionTokenId = executionTokenId
        };
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task If_KeepAliveMode_ThenReturnsValidDataValues()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest();

            // ACT
            var sut = _taskExecutionRepository;
            var response = await sut.StartAsync(startRequest);

            // ASSERT
            Assert.True(response.TaskExecutionId != 0);
            Assert.True(response.StartedAt > DateTime.MinValue);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task If_KeepAliveMode_OneTaskAndOneTokenAndIsAvailable_ThenIsGranted()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest();

            // ACT
            var response = await _taskExecutionRepository.StartAsync(startRequest);

            // ASSERT
            Assert.Equal(GrantStatusEnum.Granted, response.GrantStatus);
            Assert.NotEqual(Guid.Empty, response.ExecutionTokenId);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task If_KeepAliveMode_TwoConcurrentTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndDenyTheOther()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var firstStartRequest = CreateKeepAliveStartRequest();
            var secondStartRequest = CreateKeepAliveStartRequest();

            // ACT
            var firstResponse = await _taskExecutionRepository.StartAsync(firstStartRequest);
            await _taskExecutionRepository.SendKeepAliveAsync(CreateKeepAliveRequest(CurrentTaskId,
                firstResponse.TaskExecutionId, firstResponse.ExecutionTokenId));
            var secondResponse = await _taskExecutionRepository.StartAsync(secondStartRequest);

            // ASSERT
            Assert.Equal(GrantStatusEnum.Granted, firstResponse.GrantStatus);
            Assert.Equal(GrantStatusEnum.Denied, secondResponse.GrantStatus);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task
        If_KeepAliveMode_TwoSequentialTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndThenGrantTheOther()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var firstStartRequest = CreateKeepAliveStartRequest();
            var secondStartRequest = CreateKeepAliveStartRequest();

            // ACT
            var firstStartResponse = await _taskExecutionRepository.StartAsync(firstStartRequest);
            var firstCompleteRequest = new TaskExecutionCompleteRequest(
                CurrentTaskId, firstStartResponse.TaskExecutionId,
                firstStartResponse.ExecutionTokenId);
            var firstCompleteResponse = await _taskExecutionRepository.CompleteAsync(firstCompleteRequest);

            var secondStartResponse = await _taskExecutionRepository.StartAsync(secondStartRequest);

            // ASSERT
            Assert.Equal(GrantStatusEnum.Granted, firstStartResponse.GrantStatus);
            Assert.Equal(GrantStatusEnum.Granted, secondStartResponse.GrantStatus);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task
        If_KeepAliveMode_FiveConcurrentTasksAndFourTokensAndAllAreAvailable_ThenIsGrantFirstFourTasksAndDenyTheOther()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var concurrencyLimit = 4;

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertAvailableExecutionToken(taskDefinitionId, concurrencyLimit);

            var firstStartRequest = CreateKeepAliveStartRequest(concurrencyLimit);
            var secondStartRequest = CreateKeepAliveStartRequest(concurrencyLimit);
            var thirdStartRequest = CreateKeepAliveStartRequest(concurrencyLimit);
            var fourthStartRequest = CreateKeepAliveStartRequest(concurrencyLimit);
            var fifthStartRequest = CreateKeepAliveStartRequest(concurrencyLimit);

            // ACT
            var firstResponse = await _taskExecutionRepository.StartAsync(firstStartRequest);
            _executionsHelper.SetKeepAlive(firstResponse.TaskExecutionId);
            var secondResponse = await _taskExecutionRepository.StartAsync(secondStartRequest);
            _executionsHelper.SetKeepAlive(secondResponse.TaskExecutionId);
            var thirdResponse = await _taskExecutionRepository.StartAsync(thirdStartRequest);
            _executionsHelper.SetKeepAlive(thirdResponse.TaskExecutionId);
            var fourthResponse = await _taskExecutionRepository.StartAsync(fourthStartRequest);
            _executionsHelper.SetKeepAlive(fourthResponse.TaskExecutionId);
            var fifthResponse = await _taskExecutionRepository.StartAsync(fifthStartRequest);

            // ASSERT
            Assert.Equal(GrantStatusEnum.Granted, firstResponse.GrantStatus);
            Assert.Equal(GrantStatusEnum.Granted, secondResponse.GrantStatus);
            Assert.Equal(GrantStatusEnum.Granted, thirdResponse.GrantStatus);
            Assert.Equal(GrantStatusEnum.Granted, fourthResponse.GrantStatus);
            Assert.Equal(GrantStatusEnum.Denied, fifthResponse.GrantStatus);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public void If_KeepAliveMode_OneToken_MultipleTaskThreads_ThenNoDeadLocksOccur()
    {
        InSemaphore(() =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertAvailableExecutionToken(taskDefinitionId);

            // ACT
            var sut = _taskExecutionRepository;
            var tasks = new List<Task>();
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync()));

            Task.WaitAll(tasks.ToArray());

            // ASSERT
        });
    }

    private async Task RequestAndReturnTokenWithKeepAliveModeAsync()
    {
        for (var i = 0; i < 100; i++)
        {
            var firstStartRequest = CreateKeepAliveStartRequest();

            var firstStartResponse = await _taskExecutionRepository.StartAsync(firstStartRequest);
            _executionsHelper.SetKeepAlive(firstStartResponse.TaskExecutionId);

            if (firstStartResponse.GrantStatus == GrantStatusEnum.Granted)
            {
                var firstCompleteRequest = new TaskExecutionCompleteRequest(
                    CurrentTaskId,
                    firstStartResponse.TaskExecutionId, firstStartResponse.ExecutionTokenId);
                var firstCompleteResponse = await _taskExecutionRepository.CompleteAsync(firstCompleteRequest);
            }
        }
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task
        If_KeepAliveMode_OneTaskAndOneTokenAndIsUnavailableAndKeepAliveHasPassedElapsedTime_ThenIsGranted()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest();
            startRequest.KeepAliveDeathThreshold = TimeSpans.FourSeconds;

            var secondRequest = CreateKeepAliveStartRequest();
            secondRequest.KeepAliveDeathThreshold = TimeSpans.FourSeconds;

            // ACT
            var sut = _taskExecutionRepository;
            var firstResponse = await sut.StartAsync(startRequest);
            _executionsHelper.SetKeepAlive(firstResponse.TaskExecutionId);

            Thread.Sleep(6000);

            var secondResponse = await sut.StartAsync(secondRequest);

            // ASSERT
            Assert.Equal(GrantStatusEnum.Granted, secondResponse.GrantStatus);
            Assert.NotEqual(Guid.Empty, secondResponse.ExecutionTokenId);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task
        If_KeepAliveMode_OneTaskAndOneTokenAndIsUnavailableAndKeepAliveHasNotPassedElapsedTime_ThenIsDenied()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest();
            startRequest.KeepAliveDeathThreshold = TimeSpans.OneHour;

            var secondRequest = CreateKeepAliveStartRequest();
            secondRequest.KeepAliveDeathThreshold = TimeSpans.OneHour;

            // ACT
            var sut = _taskExecutionRepository;
            var firstResponse = await sut.StartAsync(startRequest);
            _executionsHelper.SetKeepAlive(firstResponse.TaskExecutionId);

            Thread.Sleep(5000);

            var secondResponse = await sut.StartAsync(secondRequest);

            // ASSERT
            Assert.Equal(GrantStatusEnum.Granted, firstResponse.GrantStatus);
            Assert.NotEqual(Guid.Empty, firstResponse.ExecutionTokenId);
            Assert.Equal(GrantStatusEnum.Denied, secondResponse.GrantStatus);
            Assert.Equal(Guid.Empty, secondResponse.ExecutionTokenId);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task If_KeepAliveMode_OneTokenExistsAndConcurrencyLimitIsFour_ThenCreateThreeNewTokens()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest(4);

            // ACT
            var sut = _taskExecutionRepository;
            await sut.StartAsync(startRequest);

            // ASSERT
            var tokensList = _executionsHelper.GetExecutionTokens(CurrentTaskId);
            Assert.Equal(1, tokensList.Count(x => x.Status == ExecutionTokenStatus.Unavailable));
            Assert.Equal(3, tokensList.Count(x => x.Status == ExecutionTokenStatus.Available));
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task
        If_KeepAliveMode_OneTokenExistsAndConcurrencyLimitIsUnlimited_ThenRemoveAvailableTokenAndCreateOneNewUnlimitedToken()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest(-1);

            // ACT
            var sut = _taskExecutionRepository;
            await sut.StartAsync(startRequest);

            // ASSERT
            var tokensList = _executionsHelper.GetExecutionTokens(CurrentTaskId);
            Assert.Equal(1, tokensList.Count(x => x.Status == ExecutionTokenStatus.Unlimited));
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task
        If_KeepAliveMode_OneAvailableTokenAndOneUnavailableTokensExistsAndConcurrencyLimitIsOne_ThenRemoveAvailableToken_AndSoDenyStart()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertExecutionToken(taskDefinitionId, new ExecutionTokenList
            {
                new(ExecutionTokenStatus.Unavailable, 0),
                new(ExecutionTokenStatus.Available, 1)
            });

            var startRequest = CreateKeepAliveStartRequest();

            // ACT
            var sut = _taskExecutionRepository;
            var result = await sut.StartAsync(startRequest);

            // ASSERT
            var tokensList = _executionsHelper.GetExecutionTokens(CurrentTaskId);
            Assert.Equal(GrantStatusEnum.Denied, result.GrantStatus);
            Assert.Equal(1, tokensList.Count(x => x.Status == ExecutionTokenStatus.Unavailable));
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "ExecutionTokens")]
    public async Task
        If_KeepAliveMode_TwoUnavailableTokensExistsAndConcurrencyLimitIsOne_ThenRemoveOneUnavailableToken()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            var taskDefinitionId = _executionsHelper.InsertTask(CurrentTaskId);
            _executionsHelper.InsertExecutionToken(taskDefinitionId, new ExecutionTokenList
            {
                new(ExecutionTokenStatus.Unavailable, 0),
                new(ExecutionTokenStatus.Unavailable, 1)
            });

            var startRequest = CreateKeepAliveStartRequest();

            // ACT
            var sut = _taskExecutionRepository;
            await sut.StartAsync(startRequest);

            // ASSERT
            var tokensList = _executionsHelper.GetExecutionTokens(CurrentTaskId);
            Assert.Equal(1, tokensList.Count(x => x.Status == ExecutionTokenStatus.Unavailable));
        });
    }
}