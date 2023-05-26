using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Taskling.Contexts;
using Taskling.InfrastructureContracts;

namespace Taskling.EntityFrameworkCore.Tests.Helpers;

public class ClientHelper : IClientHelper
{
    private readonly ILogger<ClientHelper> _logger;

    private readonly object _mutex = new();
    private readonly IServiceProvider _serviceProvider;

    public ClientHelper(IServiceProvider serviceProvider, ILogger<ClientHelper> logger)
    {
        _logger = logger;

        _serviceProvider = serviceProvider;

        _logger.LogDebug($"{nameof(ClientHelper)} constructor was called");
    }

    public ConfigurationOptions GetDefaultTaskConfigurationWithTimePeriodOverrideAndNoReprocessing(
        int maxBlocksToGenerate = 2000)
    {
        return new ConfigurationOptions
        {
            ConnectionString = TestConstants.GetTestConnectionString(),
            DatabaseTimeoutSeconds = 120,
            Enabled = true,
            ConcurrencyLimit = 1,
            KeepListItemsForDays = 2000,
            KeepGeneralDataForDays = 2000,
            MinimumCleanUpIntervalHours = 1,
            UseKeepAliveMode = false,
            KeepAliveIntervalMinutes = 0,
            KeepAliveDeathThresholdMinutes = 0,
            TimePeriodDeathThresholdMinutes = 240,
            ReprocessFailedTasks = false,
            FailedTaskDetectionRangeMinutes = 0,
            FailedTaskRetryLimit = 0,
            ReprocessDeadTasks = false,
            DeadTaskDetectionRangeMinutes = 0,
            DeadTaskRetryLimit = 0,
            MaxBlocksToGenerate = maxBlocksToGenerate
        };
    }

    public ITaskExecutionContext GetExecutionContext(TaskId taskId,
        ConfigurationOptions configurationOptions)
    {
        var client = CreateClient(configurationOptions);
        return client.CreateTaskExecutionContext(taskId);
    }

    public ConfigurationOptions GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(int maxBlocksToGenerate = 2000)
    {
        return new ConfigurationOptions
        {
            ConnectionString = TestConstants.GetTestConnectionString(),
            DatabaseTimeoutSeconds = 120,
            Enabled = true,
            ConcurrencyLimit = 1,
            KeepListItemsForDays = 2000,
            KeepGeneralDataForDays = 2000,
            MinimumCleanUpIntervalHours = 1,
            UseKeepAliveMode = true,
            KeepAliveIntervalMinutes = 1,
            KeepAliveDeathThresholdMinutes = 10,
            TimePeriodDeathThresholdMinutes = 0,
            ReprocessFailedTasks = true,
            FailedTaskDetectionRangeMinutes = 600,
            FailedTaskRetryLimit = 3,
            ReprocessDeadTasks = true,
            DeadTaskDetectionRangeMinutes = 600,
            DeadTaskRetryLimit = 3,
            MaxBlocksToGenerate = maxBlocksToGenerate
        };
    }

    public ConfigurationOptions GetDefaultTaskConfigurationWithKeepAliveAndNoReprocessing(
        int maxBlocksToGenerate = 2000)
    {
        var a = new ConfigurationOptions
        {
            ConnectionString = TestConstants.GetTestConnectionString(),
            DatabaseTimeoutSeconds = 120,
            Enabled = true,
            ConcurrencyLimit = 1,
            KeepListItemsForDays = 2000,
            KeepGeneralDataForDays = 2000,
            MinimumCleanUpIntervalHours = 1,
            UseKeepAliveMode = true,
            KeepAliveIntervalMinutes = 1,
            KeepAliveDeathThresholdMinutes = 2,
            TimePeriodDeathThresholdMinutes = 0,
            ReprocessFailedTasks = true,
            FailedTaskDetectionRangeMinutes = 0,
            FailedTaskRetryLimit = 0,
            ReprocessDeadTasks = false,
            DeadTaskDetectionRangeMinutes = 0,
            DeadTaskRetryLimit = 0,
            MaxBlocksToGenerate = maxBlocksToGenerate
        };
        return a;
    }

    public ConfigurationOptions GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing(
        int maxBlocksToGenerate = 2000)
    {
        return new ConfigurationOptions
        {
            ConnectionString = TestConstants.GetTestConnectionString(),
            DatabaseTimeoutSeconds = 120,
            Enabled = true,
            ConcurrencyLimit = 1,
            KeepListItemsForDays = 2000,
            KeepGeneralDataForDays = 2000,
            MinimumCleanUpIntervalHours = 1,
            UseKeepAliveMode = false,
            KeepAliveIntervalMinutes = 0,
            KeepAliveDeathThresholdMinutes = 0,
            TimePeriodDeathThresholdMinutes = 240,
            ReprocessFailedTasks = true,
            FailedTaskDetectionRangeMinutes = 600,
            FailedTaskRetryLimit = 3,
            ReprocessDeadTasks = true,
            DeadTaskDetectionRangeMinutes = 600,
            DeadTaskRetryLimit = 3,
            MaxBlocksToGenerate = maxBlocksToGenerate
        };
    }

    private TasklingClient CreateClient(ConfigurationOptions configurationOptions)
    {
        lock (_mutex)
        {
            return new TasklingClient(_serviceProvider,
                new TestTaskConfigurationReader(configurationOptions,
                    _serviceProvider.GetRequiredService<ILogger<TestTaskConfigurationReader>>()));
        }
    }
}