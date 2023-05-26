using Taskling.InfrastructureContracts;

namespace Taskling.Configuration;

public interface ITaskConfigurationReader
{
    IConfigurationOptions GetTaskConfiguration(TaskId taskId);
}