namespace Taskling.Configuration;

public interface ITaskConfigurationRepository
{
    TaskConfiguration GetTaskConfiguration(string applicationName, string taskName);
}