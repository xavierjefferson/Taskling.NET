using Taskling.InfrastructureContracts;

namespace Taskling.Configuration;

public interface IConfigurationReader
{
    ConfigurationOptions GetTaskConfigurationString(TaskId taskId);
}