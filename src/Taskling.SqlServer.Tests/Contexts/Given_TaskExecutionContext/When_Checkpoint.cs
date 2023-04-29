using System;
using System.Threading.Tasks;
using Taskling.Events;
using Taskling.SqlServer.Tests.Contexts.Given_ObjectBlockContext;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Contexts.Given_TaskExecutionContext;
[Collection(Constants.CollectionName)]
public class When_Checkpoint
{
    private readonly int _taskDefinitionId;

    public When_Checkpoint()
    {
        var executionHelper = new ExecutionsHelper();
        executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

        _taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_Checkpoint_ThenCheckpointEventCreated()
    {
        // ARRANGE
        var executionHelper = new ExecutionsHelper();

        // ACT
        bool startedOk;
        GetLastEventResponse lastEvent = null;

        using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName,
                   ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
        {
            startedOk = await executionContext.TryStartAsync();
            await executionContext.CheckpointAsync("Test checkpoint");
            lastEvent = executionHelper.GetLastEvent(_taskDefinitionId);
        }

        // ASSERT
        Assert.Equal(EventType.CheckPoint, lastEvent.EventType);
        Assert.Equal("Test checkpoint", lastEvent.Message);
    }
}