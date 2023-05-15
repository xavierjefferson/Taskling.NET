using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.SqlServer.Tests.Repositories.Given_BlockRepository;
using Xunit;

namespace Taskling.SqlServer.Tests.Contexts.Given_TaskExecutionContext;

[Collection(TestConstants.CollectionName)]
public class When_Blocked:TestBase
{
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly int _taskDefinitionId;

    public When_Blocked(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper, IClientHelper clientHelper,
        ILogger<When_Blocked> logger, ITaskRepository taskRepository) : base(executionsHelper)
    {
        _executionsHelper = executionsHelper;
        _clientHelper = clientHelper;

        executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);

        _taskDefinitionId = executionsHelper.InsertTask(CurrentTaskId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_TryStartOverTheConcurrencyLimit_ThenMarkExecutionAsBlocked()
    {
        await InSemaphoreAsync(async () =>
        {
// ARRANGE
            var executionsHelper = _executionsHelper;

            // ACT
            bool startedOk;
            bool startedOkBlockedExec;
            bool isBlocked;

            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                using (var executionContextBlocked = _clientHelper.GetExecutionContext(CurrentTaskId,
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
        });
    }
}