﻿using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.InfrastructureContracts.TaskExecution;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests.Contexts.Given_TaskExecutionContext;

[Collection(CollectionName)]
public class When_Blocked : TestBase
{
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_Blocked> _logger;
    private readonly long _taskDefinitionId;

    public When_Blocked(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper, IClientHelper clientHelper,
        ILogger<When_Blocked> logger, ITaskRepository taskRepository) : base(executionsHelper)
    {
        _logger = logger;
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