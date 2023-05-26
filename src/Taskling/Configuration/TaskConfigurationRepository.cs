using System;
using System.Collections.Generic;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;

namespace Taskling.Configuration;

public class TaskConfigurationRepository : ITaskConfigurationRepository
{
    private readonly object _cacheSync = new();
    private readonly ILogger<TaskConfigurationRepository> _logger;
    private readonly IMemoryCache _memoryCache;
    private readonly StartupOptions _options;
    private readonly ITaskConfigurationReader _taskConfigurationReader;

    private readonly Dictionary<string, TaskConfiguration> _taskConfigurations;

    public TaskConfigurationRepository(ITaskConfigurationReader taskConfigurationReader,
        ILogger<TaskConfigurationRepository> logger, IMemoryCache memoryCache, StartupOptions options)
    {
        _logger = logger;
        _memoryCache = memoryCache;
        _options = options;
        _taskConfigurationReader = taskConfigurationReader;
        _taskConfigurations = new Dictionary<string, TaskConfiguration>();
    }

    public TaskConfiguration GetTaskConfiguration(TaskId taskId)
    {
        if (string.IsNullOrEmpty(taskId.ApplicationName))
            throw new TaskConfigurationException("Cannot load a TaskConfiguration, ApplicationName is null or empty");

        if (string.IsNullOrEmpty(taskId.TaskName))
            throw new TaskConfigurationException("Cannot load a TaskConfiguration, TaskName is null or empty");

        lock (_cacheSync)
        {
            var key = GetCacheKey(taskId);
            var loadFromConfigFile = false;
            if (!_taskConfigurations.ContainsKey(key))
                loadFromConfigFile = true;
            else if ((DateTime.UtcNow - _taskConfigurations[key].DateLoaded).Minutes > 10)
                loadFromConfigFile = true;

            if (loadFromConfigFile)
            {
                var configuration = LoadConfiguration(taskId);
                //configuration.TaskId = taskId;
                configuration.DateLoaded = DateTime.UtcNow;

                if (!_taskConfigurations.ContainsKey(key))
                    _taskConfigurations.Add(key, configuration);
                else
                    _taskConfigurations[key] = configuration;
            }
            else
            {
                _logger.LogDebug($"Loading config for {key} from cache");
            }

            _logger.LogDebug(JsonConvert.SerializeObject(_taskConfigurations[key], Formatting.Indented));
            return _taskConfigurations[key];
        }
    }

    private TaskConfiguration LoadConfiguration(TaskId taskId)
    {
        var key = GetCacheKey(taskId);
        _logger.LogDebug($"Loading config for {key} from persistence");
        var configString = _taskConfigurationReader.GetTaskConfiguration(taskId);

        var taskConfiguration = ParseConfig(configString, taskId);
        return taskConfiguration;
    }

    private string GetCacheKey(TaskId taskId)
    {
        return $"config_{taskId.GetUniqueKey()}";
    }

    private TaskConfiguration ParseConfig(IConfigurationOptions source, TaskId taskId)
    {
        var connectionString =
            source.ConnectionString ??
            throw new ArgumentNullException(nameof(IConfigurationOptions.ConnectionString));
        var taskConfiguration = new TaskConfiguration(taskId)
        {
            ConnectionString = source.ConnectionString,
            DatabaseTimeoutSeconds = source.DatabaseTimeoutSeconds,
            Enabled = source.Enabled,
            ConcurrencyLimit = source.ConcurrencyLimit,
            KeepListItemsForDays = source.KeepListItemsForDays,
            KeepGeneralDataForDays = source.KeepGeneralDataForDays,
            MinimumCleanUpIntervalHours = source.MinimumCleanUpIntervalHours,
            UseKeepAliveMode = source.UseKeepAliveMode,
            KeepAliveIntervalMinutes = source.KeepAliveIntervalMinutes,
            KeepAliveDeathThresholdMinutes = source.KeepAliveDeathThresholdMinutes,
            TimePeriodDeathThresholdMinutes = source.TimePeriodDeathThresholdMinutes,
            ReprocessFailedTasks = source.ReprocessFailedTasks,
            FailedTaskDetectionRangeMinutes = source.FailedTaskDetectionRangeMinutes,
            FailedTaskRetryLimit = source.FailedTaskRetryLimit,
            ReprocessDeadTasks = source.ReprocessDeadTasks,
            DeadTaskDetectionRangeMinutes = source.DeadTaskDetectionRangeMinutes,
            DeadTaskRetryLimit = source.DeadTaskRetryLimit,
            MaxBlocksToGenerate = source.MaxBlocksToGenerate,
            MaxLengthForNonCompressedData = source.MaxLengthForNonCompressedData,
            MaxStatusReason = source.MaxStatusReason
        };

        return taskConfiguration;
    }
}