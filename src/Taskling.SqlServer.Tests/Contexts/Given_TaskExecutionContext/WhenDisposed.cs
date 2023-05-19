using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.SqlServer.Tokens.Executions;
using Xunit;

namespace Taskling.SqlServer.Tests.Contexts.Given_TaskExecutionContext;

[Collection(TestConstants.CollectionName)]
public class WhenDisposed : TestBase
{
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<WhenDisposed> _logger;

    public WhenDisposed(IClientHelper clientHelper, IExecutionsHelper executionsHelper, ILogger<WhenDisposed> logger) :
        base(executionsHelper)
    {
        _logger = logger;
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _clientHelper = clientHelper;
        _executionsHelper = executionsHelper;


        executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_InUsingBlockAndNoExecutionTokenExists_ThenExecutionTokenCreatedAutomatically()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
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
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
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
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
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
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
            _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing());
        await executionContext.TryStartAsync();
    }
}