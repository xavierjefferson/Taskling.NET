using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Taskling.Exceptions;

namespace Taskling.Configuration;

public class TaskConfigurationRepository : ITaskConfigurationRepository
{
    private readonly object _cacheSync = new();
    private readonly IConfigurationReader _configurationReader;
    private readonly Dictionary<string, TaskConfiguration> _taskConfigurations;

    public TaskConfigurationRepository(IConfigurationReader configurationReader)
    {
        _configurationReader = configurationReader;
        _taskConfigurations = new Dictionary<string, TaskConfiguration>();
    }

    public TaskConfiguration GetTaskConfiguration(string applicationName, string taskName)
    {
        if (string.IsNullOrEmpty(applicationName))
            throw new TaskConfigurationException("Cannot load a TaskConfiguration, ApplicationName is null or empty");

        if (string.IsNullOrEmpty(taskName))
            throw new TaskConfigurationException("Cannot load a TaskConfiguration, TaskName is null or empty");

        lock (_cacheSync)
        {
            var key = GetCacheKey(applicationName, taskName);
            var loadFromConfigFile = false;
            if (!_taskConfigurations.ContainsKey(key))
                loadFromConfigFile = true;
            else if ((DateTime.UtcNow - _taskConfigurations[key].DateLoaded).Minutes > 10)
                loadFromConfigFile = true;

            if (loadFromConfigFile)
            {
                var configuration = LoadConfiguration(applicationName, taskName);
                configuration.ApplicationName = applicationName;
                configuration.TaskName = taskName;
                configuration.DateLoaded = DateTime.UtcNow;

                if (!_taskConfigurations.ContainsKey(key))
                    _taskConfigurations.Add(key, configuration);
                else
                    _taskConfigurations[key] = configuration;
            }

            return _taskConfigurations[key];
        }
    }

    private string GetCacheKey(string applicationName, string taskName)
    {
        return applicationName + "::" + taskName;
    }

    private TaskConfiguration LoadConfiguration(string applicationName, string taskName)
    {
        var configString = FindKey(applicationName, taskName);
        var taskConfiguration = ParseConfig(configString, applicationName, taskName);

        return taskConfiguration;
    }

    private TaskConfiguration ParseConfig(ConfigurationOptions configurationOptions, string applicationName, string taskName)
    {
        var databaseConnString = configurationOptions.DB ?? throw new ArgumentNullException(nameof(ConfigurationOptions.DB));


        var taskConfiguration = new TaskConfiguration();
        taskConfiguration.SetDefaultValues(applicationName,
            taskName,
            databaseConnString);


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




    private ConfigurationOptions FindKey(string applicationName, string taskName)
    {
        return _configurationReader.GetTaskConfigurationString(applicationName, taskName);
    }
}