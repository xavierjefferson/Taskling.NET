namespace Taskling.Fluent.ObjectBlocks;

public class FluentObjectBlockDescriptorBase<T> : IFluentObjectBlockDescriptorBase<T>
{
    public IOverrideConfigurationDescriptor WithObject(T data)
    {
        return new FluentObjectBlockSettings<T>(data);
    }

    public IOverrideConfigurationDescriptor WithNoNewBlocks()
    {
        return new FluentObjectBlockSettings<T>();
    }

    public IReprocessScopeDescriptor Reprocess()
    {
        return new FluentObjectBlockSettings<T>();
    }
}