using System;

namespace Taskling.Fluent;

public interface IFluentBlockSettingsDescriptor
{
    IFluentBlockSettingsDescriptor ReprocessFailedTasks(TimeSpan detectionRange, int retryLimit);
    IFluentBlockSettingsDescriptor ReprocessDeadTasks(TimeSpan detectionRange, int retryLimit);
    IComplete MaximumBlocksToGenerate(int maximumNumberOfBlocks);
}