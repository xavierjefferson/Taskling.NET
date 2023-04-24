namespace Taskling.Configuration;

public interface ITasklingConfiguration
{
    TaskConfiguration GetTaskConfiguration(string applicationName, string taskName);
}