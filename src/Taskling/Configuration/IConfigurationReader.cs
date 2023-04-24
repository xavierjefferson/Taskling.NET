namespace Taskling.Configuration;

public interface IConfigurationReader
{
    string GetTaskConfigurationString(string applicationName, string taskName);
}