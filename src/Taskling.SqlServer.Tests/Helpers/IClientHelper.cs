using Taskling.Contexts;

namespace Taskling.SqlServer.Tests.Helpers;

public interface IClientHelper
{
    ConfigurationOptions GetDefaultTaskConfigurationWithTimePeriodOverrideAndNoReprocessing(
        int maxBlocksToGenerate = 2000);

    ITaskExecutionContext GetExecutionContext(string taskName, ConfigurationOptions configurationOptions);
    ConfigurationOptions GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(int maxBlocksToGenerate = 2000);
    ConfigurationOptions GetDefaultTaskConfigurationWithKeepAliveAndNoReprocessing(int maxBlocksToGenerate = 2000);

    ConfigurationOptions GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing(
        int maxBlocksToGenerate = 2000);
}