using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.EntityFrameworkCore.Tokens.Executions;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Contexts.Given_TaskExecutionContext;

[Collection(CollectionName)]
public class WhenDisposed : TestBase
{
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<WhenDisposed> _logger;

    public WhenDisposed(IClientHelper clientHelper, IExecutionsHelper executionsHelper, ILogger<WhenDisposed> logger) :
        base(executionsHelper)
    {
        _logger = logger;
        _clientHelper = clientHelper;
        _executionsHelper = executionsHelper;
        executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_InUsingBlockAndNoExecutionTokenExists_ThenExecutionTokenCreatedAutomatically()
    {
        await InSemaphoreAsync(async () =>
        {
// ARRANGE
            var executionsHelper = _executionsHelper;
            var taskDefinitionId = executionsHelper.InsertTask(CurrentTaskId);

            // ACT

            bool startedOk;
            ExecutionTokenStatus tokenStatusAfterStart;
            ExecutionTokenStatus tokenStatusAfterUsingBlock;

            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                tokenStatusAfterStart =
                    executionsHelper.GetExecutionTokenStatus(CurrentTaskId);
            }

            await Task.Delay(1000);
            tokenStatusAfterUsingBlock =
                executionsHelper.GetExecutionTokenStatus(CurrentTaskId);

            // ASSERT
            Assert.True(startedOk);
            Assert.Equal(ExecutionTokenStatus.Unavailable, tokenStatusAfterStart);
            Assert.Equal(ExecutionTokenStatus.Available, tokenStatusAfterUsingBlock);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_InUsingBlock_ThenExecutionCompletedOnEndOfBlock()
    {
        await InSemaphoreAsync(async () =>
        {
// ARRANGE
            var executionsHelper = _executionsHelper;
            var taskDefinitionId = executionsHelper.InsertTask(CurrentTaskId);
            executionsHelper.InsertAvailableExecutionToken(taskDefinitionId);

            // ACT

            bool startedOk;
            ExecutionTokenStatus tokenStatusAfterStart;
            ExecutionTokenStatus tokenStatusAfterUsingBlock;

            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                tokenStatusAfterStart =
                    executionsHelper.GetExecutionTokenStatus(CurrentTaskId);
            }

            await Task.Delay(1000);

            tokenStatusAfterUsingBlock =
                executionsHelper.GetExecutionTokenStatus(CurrentTaskId);

            // ASSERT
            Assert.True(startedOk);
            Assert.Equal(ExecutionTokenStatus.Unavailable, tokenStatusAfterStart);
            Assert.Equal(ExecutionTokenStatus.Available, tokenStatusAfterUsingBlock);
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_KeepAlive_ThenKeepAliveContinuesUntilExecutionContextDies()
    {
        await InSemaphoreAsync(async () =>
        {
// ARRANGE
            var executionsHelper = _executionsHelper;
            var taskDefinitionId = executionsHelper.InsertTask(CurrentTaskId);
            executionsHelper.InsertAvailableExecutionToken(taskDefinitionId);

            // ACT
            await StartContextWithoutUsingOrCompletedAsync();
            GC.Collect(0, GCCollectionMode.Forced); // referenceless context is collected
            Thread.Sleep(6000);

            // ASSERT
            var expectedLastKeepAliveMax = DateTime.UtcNow.AddSeconds(-5);
            var lastKeepAlive = executionsHelper.GetLastKeepAlive(taskDefinitionId);
            Assert.True(lastKeepAlive < expectedLastKeepAliveMax);
        });
    }

    private async Task StartContextWithoutUsingOrCompletedAsync()
    {
        var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
            _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing());
        await executionContext.TryStartAsync();
    }
}