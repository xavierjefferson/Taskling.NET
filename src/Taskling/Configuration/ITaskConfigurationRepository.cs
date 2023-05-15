using Taskling.InfrastructureContracts;

namespace Taskling.Configuration;

public interface ITaskConfigurationRepository
{
    TaskConfiguration GetTaskConfiguration(TaskId taskId);
}