using Microsoft.Extensions.Logging;
using Taskling.Configuration;
using Taskling.InfrastructureContracts;

namespace Taskling.EntityFrameworkCore.Tests.Helpers;

public class TestTaskConfigurationReader : ITaskConfigurationReader
{
    private readonly IConfigurationOptions _configurationOptions;
    private readonly ILogger<TestTaskConfigurationReader> _logger;

    public TestTaskConfigurationReader(IConfigurationOptions configurationOptions,
        ILogger<TestTaskConfigurationReader> logger)
    {
        _logger = logger;
        _configurationOptions = configurationOptions;
    }

    public IConfigurationOptions GetTaskConfiguration(TaskId taskId)
    {
        return
            _configurationOptions; // "ConnectionString(Server=(local);Database=TasklingDb;Trusted_Connection=True;) DatabaseTimeoutSeconds(120) Enabled(true) ConcurrencyLimit(-1) KeepListItemsForDays(2) KeepGeneralDataForDays(40) MinimumCleanUpIntervalHours(1) UseKeepAliveMode(true) KeepAliveIntervalMinutes(1) KeepAliveDeathThresholdMinutes(10) TimePeriodDeathThresholdMinutes(0) ReprocessFailedTasks(true) RPC_FAIL_MTS(600) FailedTaskRetryLimit(3) ReprocessDeadTasks(true) RPC_DEAD_MTS(600) DeadTaskRetryLimit(3) MaxBlocksToGenerate(20)";
    }
}