using System;
using System.Threading.Tasks;

namespace Taskling.Blocks.ListBlocks;

public interface IItem
{
    ItemStatus Status { get; set; }
}
public interface IListBlockItem<T> : IItem
{
    long ListBlockItemId { get; }
    T Value { get; }

    string StatusReason { get; set; }
    DateTime LastUpdated { get; }
    int? Step { get; set; }

    Task CompleteAsync();
    Task FailedAsync(string message);
    Task DiscardedAsync(string message);
    void Failed(string message);
    void Discarded(string message);
    void Complete();
}