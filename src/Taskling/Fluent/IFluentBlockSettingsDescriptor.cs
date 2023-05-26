using System;

namespace Taskling.Fluent;

public interface IFluentBlockSettingsDescriptor
{
    IFluentBlockSettingsDescriptor WithReprocessFailedTasks(TimeSpan detectionRange, int retryLimit);
    IFluentBlockSettingsDescriptor WithReprocessDeadTasks(TimeSpan detectionRange, int retryLimit);
    IComplete WithMaximumBlocksToGenerate(int maximumNumberOfBlocks);
}