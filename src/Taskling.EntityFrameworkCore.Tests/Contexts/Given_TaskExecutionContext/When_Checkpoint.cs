using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.Enums;
using Taskling.InfrastructureContracts.TaskExecution;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Contexts.Given_TaskExecutionContext;

[Collection(TestConstants.CollectionName)]
public class When_Checkpoint : TestBase
{
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_Checkpoint> _logger;
    private readonly long _taskDefinitionId;

    public When_Checkpoint(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper, IClientHelper clientHelper,
        ILogger<When_Checkpoint> logger, ITaskRepository taskRepository) : base(executionsHelper)
    {
        _logger = logger;
        _clientHelper = clientHelper;

        _executionsHelper = executionsHelper;
        executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);

        _taskDefinitionId = executionsHelper.InsertTask(CurrentTaskId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_Checkpoint_ThenCheckpointEventCreated()
    {
        await InSemaphoreAsync(async () =>
        {
// ARRANGE
            var executionsHelper = _executionsHelper;

            // ACT
            bool startedOk;
            GetLastEventResponse lastEvent = null;

            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                await executionContext.CheckpointAsync("Test checkpoint");
                lastEvent = executionsHelper.GetLastEvent(_taskDefinitionId);
            }

            // ASSERT
            Assert.Equal(EventTypeEnum.CheckPoint, lastEvent.EventType);
            Assert.Equal("Test checkpoint", lastEvent.Message);
        });
    }
}