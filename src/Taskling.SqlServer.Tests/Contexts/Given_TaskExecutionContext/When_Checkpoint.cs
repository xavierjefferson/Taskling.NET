using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.Events;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Contexts.Given_TaskExecutionContext;

[Collection(TestConstants.CollectionName)]
public class When_Checkpoint
{
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly int _taskDefinitionId;

    public When_Checkpoint(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper, IClientHelper clientHelper,
        ILogger<When_Checkpoint> logger, ITaskRepository taskRepository)
    {
        _clientHelper = clientHelper;
        _executionsHelper = executionsHelper;
        executionsHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

        _taskDefinitionId = executionsHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_Checkpoint_ThenCheckpointEventCreated()
    {
        // ARRANGE
        var executionsHelper = _executionsHelper;

        // ACT
        bool startedOk;
        GetLastEventResponse lastEvent = null;

        using (var executionContext = _clientHelper.GetExecutionContext(TestConstants.TaskName,
                   _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
        {
            startedOk = await executionContext.TryStartAsync();
            await executionContext.CheckpointAsync("Test checkpoint");
            lastEvent = executionsHelper.GetLastEvent(_taskDefinitionId);
        }

        // ASSERT
        Assert.Equal(EventType.CheckPoint, lastEvent.EventType);
        Assert.Equal("Test checkpoint", lastEvent.Message);
    }
}