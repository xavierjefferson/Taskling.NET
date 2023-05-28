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
            var loadFromReader = false;
            if (!_taskConfigurations.ContainsKey(key))
                loadFromReader = true;
            else if (DateTime.UtcNow - _taskConfigurations[key].DateLoaded > TimeSpan.FromSeconds(_taskConfigurations[key].ExpiresInSeconds))
                loadFromReader = true;

            if (loadFromReader)
            {
                var configuration = LoadConfiguration(taskId);
                if (configuration.ExpiresInSeconds > 0)
                {
                    if (!_taskConfigurations.ContainsKey(key))
                        _taskConfigurations.Add(key, configuration);
                    else
                        _taskConfigurations[key] = configuration;
                }
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
        var configurationOptions = _taskConfigurationReader.GetTaskConfiguration(taskId);
        return new TaskConfiguration(taskId, configurationOptions);
    }

    private string GetCacheKey(TaskId taskId)
    {
        return $"config_{taskId.GetUniqueKey()}";
    }

}