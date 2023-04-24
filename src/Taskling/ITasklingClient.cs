using Taskling.Contexts;

namespace Taskling;

public interface ITasklingClient
{
    ITaskExecutionContext CreateTaskExecutionContext(string applicationName, string taskName);
}