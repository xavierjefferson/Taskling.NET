using System;
using Taskling.Blocks.ListBlocks;

namespace Taskling.InfrastructureContracts.Blocks.ListBlocks;

public interface IListBlockUpdateArgs
{
    long ListBlockItemId { get; set; }
    ItemStatus Status { get; set; }
    string StatusReason { get; set; }
    int? Step { get; set; }
}

public class ProtoListBlockItem : IListBlockUpdateArgs
{
    public long ListBlockItemId { get; set; }
    public string Value { get; set; }
    public ItemStatus Status { get; set; }
    public string StatusReason { get; set; }
    public DateTime LastUpdated { get; set; }
    public int? Step { get; set; }
}