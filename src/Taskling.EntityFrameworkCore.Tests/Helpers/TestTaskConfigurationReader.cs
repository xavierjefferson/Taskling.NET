using System;
using System.Collections.Generic;
using Taskling.Configuration;
using Taskling.InfrastructureContracts;

namespace Taskling.EntityFrameworkCore.Tests.Helpers;

public class TestTaskConfigurationReader : ITaskConfigurationReader
{
    private readonly Dictionary<TaskId, IConfigurationOptions> _configurationOptionsDictionary = new();

    public IConfigurationOptions GetTaskConfiguration(TaskId taskId)
    {
        if (_configurationOptionsDictionary.ContainsKey(taskId))
        {
            return _configurationOptionsDictionary[taskId];
        }

        return new ConfigurationOptions()
            { ConnectionString = Startup.GetConnectionString(), CommandTimeoutSeconds = 120, ExpiresInSeconds = 0 };
    }

    public void Add(TaskId taskId, ConfigurationOptions configurationOptions)
    {
        _configurationOptionsDictionary[taskId] = configurationOptions;
    }
}