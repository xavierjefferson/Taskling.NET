using Taskling.Configuration;
using Taskling.InfrastructureContracts;

namespace Taskling.CleanUp;

public interface ICleanUpService
{
    void CleanOldData(TaskId taskId, long taskExecutionId,
        ITaskConfigurationRepository taskConfigurationRepository);
}