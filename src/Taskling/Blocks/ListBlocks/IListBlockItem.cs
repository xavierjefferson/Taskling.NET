using System;
using System.Threading.Tasks;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;

namespace Taskling.Blocks.ListBlocks;

public interface IListBlockItem<T>
{
    long ListBlockItemId { get; }
    T Value { get; }
    ItemStatus Status { get; set; }
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