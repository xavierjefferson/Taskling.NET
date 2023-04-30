namespace Taskling.Configuration;

public interface IConfig
{
    string DB { get; }
}
public interface IConfigurationReader
{
    ConfigurationOptions GetTaskConfigurationString(string applicationName, string taskName);
}