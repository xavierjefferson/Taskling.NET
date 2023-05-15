using System;

namespace Taskling.Fluent;

public interface IReprocessTaskDescriptor
{
    IComplete OfExecutionWith(Guid referenceValue);
}

public interface IReprocessScopeDescriptor
{
    IReprocessTaskDescriptor AllBlocks();
    IReprocessTaskDescriptor PendingAndFailedBlocks();
}