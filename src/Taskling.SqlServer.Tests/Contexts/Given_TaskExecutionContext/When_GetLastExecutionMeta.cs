using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.SqlServer.Tests.Repositories.Given_BlockRepository;
using Xunit;
using TaskExecutionStatus = Taskling.Tasks.TaskExecutionStatus;

namespace Taskling.SqlServer.Tests.Contexts.Given_TaskExecutionContext;

[Collection(TestConstants.CollectionName)]
public class When_GetLastExecutionMeta : TestBase
{
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly int _taskDefinitionId;

    public When_GetLastExecutionMeta(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper,
        IClientHelper clientHelper, ILogger<When_GetLastExecutionMeta> logger, ITaskRepository taskRepository) : base(executionsHelper)
    {
        _executionsHelper = executionsHelper;
        _clientHelper = clientHelper;

        executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);

        _taskDefinitionId = executionsHelper.InsertTask(CurrentTaskId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_MultipleExecutionsAndGetLastExecutionMeta_ThenReturnLastOne()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var referenceValues = new[]
            {
            Guid.Parse("cfed1331-24cc-4fe0-a308-4f5f88af3df4"),
            Guid.Parse("eda35fa8-5742-45c0-bab1-7be557445559"),
            Guid.Parse("785a4118-223b-44c1-8709-0fb4d10ef21c"),
            Guid.Parse("79d2de63-563d-4464-afde-07506e9cf56d"),
            Guid.Parse("902f1c0d-100c-407e-a744-c9e09c2e5e69"),
        };
            foreach (var referenceValue in referenceValues)
            {
                using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                           _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    await executionContext.TryStartAsync(referenceValue);
                }

                Thread.Sleep(200);
            }

            // ACT and ASSERT
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync();
                Assert.Equal(referenceValues[4], executionMeta.ReferenceValue);
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_MultipleExecutionsAndGetLastExecutionMetas_ThenReturnLastXItems()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var referenceValues = new[]
            {
            Guid.Parse("cfed1331-24cc-4fe0-a308-4f5f88af3df4"),
            Guid.Parse("eda35fa8-5742-45c0-bab1-7be557445559"),
            Guid.Parse("785a4118-223b-44c1-8709-0fb4d10ef21c"),
            Guid.Parse("79d2de63-563d-4464-afde-07506e9cf56d"),
            Guid.Parse("902f1c0d-100c-407e-a744-c9e09c2e5e69"),
        };
            foreach (var referenceValue in referenceValues)
            {
                using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                           _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    await executionContext.TryStartAsync(referenceValue);
                }

                Thread.Sleep(200);
            }

            // ACT and ASSERT
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMetas = await executionContext.GetLastExecutionMetasAsync(3);
                Assert.Equal(3, executionMetas.Count);
                Assert.Equal(referenceValues[4], executionMetas[0].ReferenceValue);
                Assert.Equal(referenceValues[3], executionMetas[1].ReferenceValue);
                Assert.Equal(referenceValues[2], executionMetas[2].ReferenceValue);
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_NoPreviousExecutionsAndGetLastExecutionMeta_ThenReturnNull()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            // ACT and ASSERT
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync();
                Assert.Null(executionMeta);
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_MultipleExecutionsAndGetLastExecutionMetaWithHeader_ThenReturnLastOne()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            for (var i = 0; i < 5; i++)
            {
                var myHeader = new MyHeader
                {
                    Name = "Test",
                    Id = i
                };

                using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                           _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    await executionContext.TryStartAsync(myHeader);
                }

                Thread.Sleep(200);
            }

            // ACT and ASSERT
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync<MyHeader>();
                Assert.Equal(4, executionMeta.Header.Id);
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_MultipleExecutionsAndGetLastExecutionMetasWithHeader_ThenReturnLastXItems()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE

            for (var i = 0; i < 5; i++)
            {
                var myHeader = new MyHeader
                {
                    Name = "Test",
                    Id = i
                };

                using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                           _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    await executionContext.TryStartAsync(myHeader);
                }

                Thread.Sleep(200);
            }

            // ACT and ASSERT
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMetas = await executionContext.GetLastExecutionMetasAsync<MyHeader>(3);
                Assert.Equal(3, executionMetas.Count);
                Assert.Equal(4, executionMetas[0].Header.Id);
                Assert.Equal(3, executionMetas[1].Header.Id);
                Assert.Equal(2, executionMetas[2].Header.Id);
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_NoPreviousExecutionsAndGetLastExecutionMetaWithHeader_ThenReturnNull()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            // ACT

            // ASSERT
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync<MyHeader>();
                Assert.Null(executionMeta);
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_LastExecutionCompleted_ThenReturnStatusIsCompleted()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                await executionContext.TryStartAsync();
            }

            Thread.Sleep(200);


            // ACT and ASSERT
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync();
                Assert.Equal(TaskExecutionStatus.Completed, executionMeta.Status);
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_LastExecutionFailed_ThenReturnStatusIsFailed()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                await executionContext.TryStartAsync();
                await executionContext.ErrorAsync("", true);
            }

            Thread.Sleep(200);


            // ACT and ASSERT
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync();
                Assert.Equal(TaskExecutionStatus.Failed, executionMeta.Status);
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_LastExecutionBlocked_ThenReturnStatusIsBlockedAsync()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                await executionContext.TryStartAsync();
                Thread.Sleep(200);

                using (var executionContext2 = _clientHelper.GetExecutionContext(CurrentTaskId,
                           _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    await executionContext2.TryStartAsync();
                    await executionContext2.CompleteAsync();
                }

                await executionContext.CompleteAsync();
            }


            // ACT and ASSERT
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync();
                Assert.Equal(TaskExecutionStatus.Blocked, executionMeta.Status);

                await executionContext.CompleteAsync();
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_LastExecutionInProgress_ThenReturnStatusIsInProgress()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE, ACT, ASSERT
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                await executionContext.TryStartAsync();
                Thread.Sleep(200);

                using (var executionContext2 = _clientHelper.GetExecutionContext(CurrentTaskId,
                           _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    var executionMeta = await executionContext2.GetLastExecutionMetaAsync();
                    Assert.Equal(TaskExecutionStatus.InProgress, executionMeta.Status);
                }
            }
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_LastExecutionDead_ThenReturnStatusIsDead()
    {
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE, ACT, ASSERT
            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                await executionContext.TryStartAsync();
                Thread.Sleep(200);
                var helper = _executionsHelper;
                helper.SetLastExecutionAsDead(_taskDefinitionId);

                using (var executionContext2 = _clientHelper.GetExecutionContext(CurrentTaskId,
                           _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    var executionMeta = await executionContext2.GetLastExecutionMetaAsync();
                    Assert.Equal(TaskExecutionStatus.Dead, executionMeta.Status);
                }
            }
        });
    }
}