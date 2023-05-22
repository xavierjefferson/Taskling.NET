namespace Taskling.Fluent.ObjectBlocks;

public interface IFluentObjectBlockDescriptorBase<T>
{
    IOverrideConfigurationDescriptor WithObject(T data);
    IOverrideConfigurationDescriptor WithNoNewBlocks();
    IReprocessScopeDescriptor Reprocess();
}