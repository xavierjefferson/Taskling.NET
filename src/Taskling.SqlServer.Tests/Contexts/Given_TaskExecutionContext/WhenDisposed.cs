using System;
using System.Threading;
using System.Threading.Tasks;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.SqlServer.Tokens.Executions;
using Xunit;

namespace Taskling.SqlServer.Tests.Contexts.Given_TaskExecutionContext;

[Collection(TestConstants.CollectionName)]
public class WhenDisposed
{
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;

    public WhenDisposed(IClientHelper clientHelper, IExecutionsHelper executionsHelper)
    {
        _clientHelper = clientHelper;
        _executionsHelper = executionsHelper;

        executionsHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_InUsingBlockAndNoExecutionTokenExists_ThenExecutionTokenCreatedAutomatically()
    {
        // ARRANGE
        var executionsHelper = _executionsHelper;
        var taskDefinitionId = executionsHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);

        // ACT

        bool startedOk;
        ExecutionTokenStatus tokenStatusAfterStart;
        ExecutionTokenStatus tokenStatusAfterUsingBlock;

        using (var executionContext = _clientHelper.GetExecutionContext(TestConstants.TaskName,
                   _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
        {
            startedOk = await executionContext.TryStartAsync();
            tokenStatusAfterStart =
                executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);
        }

        await Task.Delay(1000);
        tokenStatusAfterUsingBlock =
            executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);

        // ASSERT
        Assert.True(startedOk);
        Assert.Equal(ExecutionTokenStatus.Unavailable, tokenStatusAfterStart);
        Assert.Equal(ExecutionTokenStatus.Available, tokenStatusAfterUsingBlock);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_InUsingBlock_ThenExecutionCompletedOnEndOfBlock()
    {
        // ARRANGE
        var executionsHelper = _executionsHelper;
        var taskDefinitionId = executionsHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionsHelper.InsertAvailableExecutionToken(taskDefinitionId);

        // ACT

        bool startedOk;
        ExecutionTokenStatus tokenStatusAfterStart;
        ExecutionTokenStatus tokenStatusAfterUsingBlock;

        using (var executionContext = _clientHelper.GetExecutionContext(TestConstants.TaskName,
                   _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
        {
            startedOk = await executionContext.TryStartAsync();
            tokenStatusAfterStart =
                executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);
        }

        await Task.Delay(1000);

        tokenStatusAfterUsingBlock =
            executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);

        // ASSERT
        Assert.True(startedOk);
        Assert.Equal(ExecutionTokenStatus.Unavailable, tokenStatusAfterStart);
        Assert.Equal(ExecutionTokenStatus.Available, tokenStatusAfterUsingBlock);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_KeepAlive_ThenKeepAliveContinuesUntilExecutionContextDies()
    {
        // ARRANGE
        var executionsHelper = _executionsHelper;
        var taskDefinitionId = executionsHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionsHelper.InsertAvailableExecutionToken(taskDefinitionId);

        // ACT
        await StartContextWithoutUsingOrCompletedAsync();
        GC.Collect(0, GCCollectionMode.Forced); // referenceless context is collected
        Thread.Sleep(6000);

        // ASSERT
        var expectedLastKeepAliveMax = DateTime.UtcNow.AddSeconds(-5);
        var lastKeepAlive = executionsHelper.GetLastKeepAlive(taskDefinitionId);
        Assert.True(lastKeepAlive < expectedLastKeepAliveMax);
    }

    private async Task StartContextWithoutUsingOrCompletedAsync()
    {
        var executionContext = _clientHelper.GetExecutionContext(TestConstants.TaskName,
            _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing());
        await executionContext.TryStartAsync();
    }
}