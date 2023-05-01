namespace Taskling.Configuration;

public interface IConfigurationReader
{
    ConfigurationOptions GetTaskConfigurationString(string applicationName, string taskName);
}