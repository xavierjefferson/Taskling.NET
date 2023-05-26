using System;
using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.ListBlocks;

public interface IListBlockUpdateArgs
{
    long ListBlockItemId { get; set; }
    ItemStatusEnum Status { get; set; }
    string StatusReason { get; set; }
    int? Step { get; set; }
}

public class ProtoListBlockItem : IListBlockUpdateArgs
{
    public string Value { get; set; }
    public DateTime LastUpdated { get; set; }
    public long ListBlockItemId { get; set; }
    public ItemStatusEnum Status { get; set; }
    public string StatusReason { get; set; }
    public int? Step { get; set; }
}