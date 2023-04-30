using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Contexts.Given_TaskExecutionContext;

[Collection(Constants.CollectionName)]
public class When_Blocked
{
    private readonly IExecutionsHelper _executionsHelper;
    private readonly IClientHelper _clientHelper;
    private readonly int _taskDefinitionId;

    public When_Blocked(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper, IClientHelper clientHelper,
        ILogger<When_Blocked> logger, ITaskRepository taskRepository)
    {
        _executionsHelper = executionsHelper;
        _clientHelper = clientHelper;

        executionsHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

        _taskDefinitionId = executionsHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_TryStartOverTheConcurrencyLimit_ThenMarkExecutionAsBlocked()
    {
        // ARRANGE
        var executionsHelper = _executionsHelper;

        // ACT
        bool startedOk;
        bool startedOkBlockedExec;
        bool isBlocked;

        using (var executionContext = _clientHelper.GetExecutionContext(TestConstants.TaskName,
                   _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
        {
            startedOk = await executionContext.TryStartAsync();
            using (var executionContextBlocked = _clientHelper.GetExecutionContext(TestConstants.TaskName,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOkBlockedExec = await executionContextBlocked.TryStartAsync();
            }

            isBlocked = executionsHelper.GetBlockedStatusOfLastExecution(_taskDefinitionId);
        }

        // ASSERT
        Assert.True(isBlocked);
        Assert.True(startedOk);
        Assert.False(startedOkBlockedExec);
    }
}