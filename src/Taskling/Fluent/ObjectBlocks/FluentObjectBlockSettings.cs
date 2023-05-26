using Taskling.Enums;
using Taskling.Fluent.Settings;

namespace Taskling.Fluent.ObjectBlocks;

public class FluentObjectBlockSettings<T> : FluentBlockSettingsDescriptor, IObjectBlockSettings<T>
{
    public FluentObjectBlockSettings()
        : base(BlockTypeEnum.Object)
    {
    }

    public FluentObjectBlockSettings(T objectData)
        : base(BlockTypeEnum.Object)
    {
        Object = objectData;
    }

    public T Object { get; set; }
}