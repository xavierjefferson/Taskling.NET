using System;
using System.Threading.Tasks;

namespace Taskling.Blocks.ListBlocks;

public interface IListBlockItem<T>
{
    long ListBlockItemId { get; }
    T Value { get; }
    ItemStatus Status { get; set; }
    string StatusReason { get; set; }
    DateTime LastUpdated { get; }
    int? Step { get; set; }

    Task CompletedAsync();
    Task FailedAsync(string message);
    Task DiscardedAsync(string message);
    void Failed(string toString);
    void Discarded(string discardedDueToDistanceRule);
    void Completed();
}