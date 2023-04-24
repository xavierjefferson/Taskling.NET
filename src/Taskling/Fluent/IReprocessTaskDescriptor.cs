namespace Taskling.Fluent;

public interface IReprocessTaskDescriptor
{
    IComplete OfExecutionWith(string referenceValue);
}

public interface IReprocessScopeDescriptor
{
    IReprocessTaskDescriptor AllBlocks();
    IReprocessTaskDescriptor PendingAndFailedBlocks();
}