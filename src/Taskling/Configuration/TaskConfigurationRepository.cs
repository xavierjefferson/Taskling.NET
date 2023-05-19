using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Taskling.Exceptions;
using Taskling.Extensions;
using Taskling.InfrastructureContracts;

namespace Taskling.Configuration;

public class TaskConfigurationRepository : ITaskConfigurationRepository
{
    private readonly object _cacheSync = new();
    private readonly IConfigurationReader _configurationReader;
    private readonly ILogger<TaskConfigurationRepository> _logger;
    private readonly Dictionary<string, TaskConfiguration> _taskConfigurations;

    public TaskConfigurationRepository(IConfigurationReader configurationReader,
        ILogger<TaskConfigurationRepository> logger)
    {
        _logger = logger;
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _configurationReader = configurationReader;
        _taskConfigurations = new Dictionary<string, TaskConfiguration>();
    }

    public TaskConfiguration GetTaskConfiguration(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
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

            return _taskConfigurations[key];
        }
    }


    private string GetCacheKey(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return taskId.GetUniqueKey();
    }

    private TaskConfiguration LoadConfiguration(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var configString = FindKey(taskId);
        var taskConfiguration = ParseConfig(configString, taskId);
        _logger.Debug("bd627edb-a956-4d6f-8461-20567b19c796");
        _logger.Debug(JsonConvert.SerializeObject(taskConfiguration, Formatting.Indented));
        return taskConfiguration;
    }

    private TaskConfiguration ParseConfig(ConfigurationOptions configurationOptions, TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var databaseConnString =
            configurationOptions.DB ?? throw new ArgumentNullException(nameof(ConfigurationOptions.DB));
        var taskConfiguration = new TaskConfiguration(taskId);
        taskConfiguration.SetDefaultValues(databaseConnString);

        taskConfiguration.ConcurrencyLimit = configurationOptions.CON;
        taskConfiguration.DatabaseTimeoutSeconds = configurationOptions.TO;
        taskConfiguration.Enabled = configurationOptions.E;
        taskConfiguration.KeepAliveDeathThresholdMinutes = configurationOptions.KADT;
        taskConfiguration.KeepAliveIntervalMinutes = configurationOptions.KAINT;
        taskConfiguration.KeepGeneralDataForDays = configurationOptions.KPDT;
        taskConfiguration.KeepListItemsForDays = configurationOptions.KPLT;
        taskConfiguration.MinimumCleanUpIntervalHours = configurationOptions.MCI;
        taskConfiguration.TimePeriodDeathThresholdMinutes = configurationOptions.TPDT;
        taskConfiguration.UsesKeepAliveMode = configurationOptions.KA;
        taskConfiguration.ReprocessFailedTasks = configurationOptions.RPC_FAIL;
        taskConfiguration.ReprocessFailedTasksDetectionRange = TimeSpan.FromMinutes(configurationOptions.RPC_DEAD_MTS);
        taskConfiguration.FailedTaskRetryLimit = configurationOptions.RPC_FAIL_RTYL;
        taskConfiguration.ReprocessDeadTasks = configurationOptions.RPC_DEAD;
        taskConfiguration.ReprocessDeadTasksDetectionRange = TimeSpan.FromMinutes(configurationOptions.RPC_DEAD_MTS);
        taskConfiguration.DeadTaskRetryLimit = configurationOptions.RPC_DEAD_RTYL;
        taskConfiguration.MaxBlocksToGenerate = configurationOptions.MXBL;
        taskConfiguration.MaxLengthForNonCompressedData =
            configurationOptions.MXCOMP <= 0 ? int.MaxValue : configurationOptions.MXCOMP;
        taskConfiguration.MaxStatusReason = configurationOptions.MXRSN;

        return taskConfiguration;
    }


    private ConfigurationOptions FindKey(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return _configurationReader.GetTaskConfigurationString(taskId);
    }
}