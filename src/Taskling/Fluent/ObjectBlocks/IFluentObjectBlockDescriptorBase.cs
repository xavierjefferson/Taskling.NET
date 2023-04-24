namespace Taskling.Fluent;

public interface IFluentObjectBlockDescriptorBase<T>
{
    IOverrideConfigurationDescriptor WithObject(T data);
    IOverrideConfigurationDescriptor WithNoNewBlocks();
    IReprocessScopeDescriptor Reprocess();
}