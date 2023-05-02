using Microsoft.Extensions.Configuration;
using Taskling;
using Taskling.Configuration;

namespace TasklingTester.Configuration;

public class TasklingIConfigurationReader : IConfigurationReader
{
    private readonly ConfigWrapper _configWrapper;

    public TasklingIConfigurationReader(IConfiguration configuration)
    {
        var _section = configuration.GetSection("Taskling");
        if (_section.Exists()) throw new InvalidOperationException("Configuration is missing section 'Taskling'");
        _configWrapper = _section.Get<ConfigWrapper>();
    }

    public ConfigurationOptions GetTaskConfigurationString(string applicationName, string taskName)
    {
        var key = applicationName + "::" + taskName;
        var configString = _configWrapper.TaskConfigurations[key];

        return configString;
    }

    private class ConfigWrapper
    {
        public Dictionary<string, ConfigurationOptions> TaskConfigurations { get; set; } = new();
    }
}