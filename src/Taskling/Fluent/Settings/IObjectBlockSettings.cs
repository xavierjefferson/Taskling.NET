namespace Taskling.Fluent.Settings;

public interface IObjectBlockSettings<T> : IBlockSettings
{
    T Object { get; set; }
}