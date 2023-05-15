using Taskling.Contexts;
using Taskling.InfrastructureContracts;

namespace Taskling;

public interface ITasklingClient
{
    ITaskExecutionContext CreateTaskExecutionContext(string applicationName, string taskName);
    ITaskExecutionContext CreateTaskExecutionContext(TaskId taskId);
}