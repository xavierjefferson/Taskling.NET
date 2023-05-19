using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests.Contexts.Given_TaskExecutionContext;

[Collection(TestConstants.CollectionName)]
public class When_Start : TestBase
{
    private readonly IClientHelper _clientHelper;
    private readonly IExecutionsHelper _executionsHelper;
    private readonly ILogger<When_Start> _logger;
    private readonly int _taskDefinitionId;

    public When_Start(IBlocksHelper blocksHelper, IExecutionsHelper executionsHelper, IClientHelper clientHelper,
        ILogger<When_Start> logger, ITaskRepository taskRepository) : base(executionsHelper)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger = logger;
        _executionsHelper = executionsHelper;
        _clientHelper = clientHelper;

        executionsHelper.DeleteRecordsOfApplication(CurrentTaskId.ApplicationName);

        _taskDefinitionId = executionsHelper.InsertTask(CurrentTaskId);
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_TryStart_ThenLogCorrectTasklingVersion()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var executionsHelper = _executionsHelper;

            // ACT
            bool startedOk;

            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                var sqlServerImplAssembly =
                    AppDomain.CurrentDomain.GetAssemblies()
                        .First(x => x.FullName.Contains("Taskling")
                                    && !x.FullName.Contains("Taskling.Sql")
                                    && !x.FullName.Contains("Tests"));
                var fileVersionInfo = FileVersionInfo.GetVersionInfo(sqlServerImplAssembly.Location);
                var versionOfTaskling = fileVersionInfo.ProductVersion;
                var executionVersion = executionsHelper.GetLastExecutionVersion(_taskDefinitionId);
                Assert.Equal(versionOfTaskling.Trim(), executionVersion.Trim());
            }

            // ASSERT
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_TryStartWithHeader_ThenGetHeaderReturnsTheHeader()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var executionsHelper = _executionsHelper;
            var myHeader = new MyHeader
            {
                Name = "Jack",
                Id = 367
            };

            // ACT
            bool startedOk;

            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync(myHeader);

                var myHeaderBack = executionContext.GetHeader<MyHeader>();
                Assert.Equal(myHeader.Name, myHeaderBack.Name);
                Assert.Equal(myHeader.Id, myHeaderBack.Id);
            }

            // ASSERT
        });
    }

    [Fact]
    [Trait("Speed", "Fast")]
    [Trait("Area", "TaskExecutions")]
    public async Task If_TryStartWithHeader_ThenHeaderWrittenToDatabase()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await InSemaphoreAsync(async () =>
        {
            // ARRANGE
            var executionsHelper = _executionsHelper;
            var myHeader = new MyHeader
            {
                Name = "Jack",
                Id = 367
            };

            // ACT
            bool startedOk;

            using (var executionContext = _clientHelper.GetExecutionContext(CurrentTaskId,
                       _clientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync(myHeader);

                var myHeaderBack = executionContext.GetHeader<MyHeader>();
            }

            var dbHelper = executionsHelper;
            var executionHeader = dbHelper.GetLastExecutionHeader(_taskDefinitionId);
            var expectedHeader = "{\"Name\":\"Jack\",\"Id\":367}";

            // ASSERT
            Assert.Equal(expectedHeader, executionHeader);
        });
    }
}