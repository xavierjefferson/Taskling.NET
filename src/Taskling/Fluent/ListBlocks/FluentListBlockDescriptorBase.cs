using System.Collections.Generic;
using Taskling.Enums;
using Taskling.Serialization;

namespace Taskling.Fluent.ListBlocks;

public class FluentListBlockDescriptorBase<T> : IFluentListBlockDescriptorBase<T>
{
    public IOverrideConfigurationDescriptor WithSingleUnitCommit(List<T> values, int maxBlockSize)
    {
        var jsonValues = Serialize(values);
        var listBlockDescriptor = new FluentBlockSettingsDescriptor(jsonValues, maxBlockSize);
        listBlockDescriptor.ListUpdateMode = ListUpdateModeEnum.SingleItemCommit;

        return listBlockDescriptor;
    }

    public IOverrideConfigurationDescriptor WithPeriodicCommit(List<T> values, int maxBlockSize,
        BatchSizeEnum batchSize)
    {
        var jsonValues = Serialize(values);
        var listBlockDescriptor = new FluentBlockSettingsDescriptor(jsonValues, maxBlockSize);
        listBlockDescriptor.ListUpdateMode = ListUpdateModeEnum.PeriodicBatchCommit;

        switch (batchSize)
        {
            case BatchSizeEnum.NotSet:
                listBlockDescriptor.UncommittedItemsThreshold = 100;
                break;
            case BatchSizeEnum.Ten:
                listBlockDescriptor.UncommittedItemsThreshold = 10;
                break;
            case BatchSizeEnum.Fifty:
                listBlockDescriptor.UncommittedItemsThreshold = 50;
                break;
            case BatchSizeEnum.Hundred:
                listBlockDescriptor.UncommittedItemsThreshold = 100;
                break;
            case BatchSizeEnum.FiveHundred:
                listBlockDescriptor.UncommittedItemsThreshold = 500;
                break;
        }

        return listBlockDescriptor;
    }

    public IOverrideConfigurationDescriptor WithBatchCommitAtEnd(List<T> values, int maxBlockSize)
    {
        var jsonValues = Serialize(values);
        var listBlockDescriptor = new FluentBlockSettingsDescriptor(jsonValues, maxBlockSize);
        listBlockDescriptor.ListUpdateMode = ListUpdateModeEnum.BatchCommitAtEnd;

        return listBlockDescriptor;
    }

    public IReprocessScopeDescriptor WithReprocessSingleUnitCommit()
    {
        var listBlockDescriptor = new FluentBlockSettingsDescriptor(BlockTypeEnum.List);
        listBlockDescriptor.ListUpdateMode = ListUpdateModeEnum.SingleItemCommit;

        return listBlockDescriptor;
    }

    public IReprocessScopeDescriptor WithReprocessPeriodicCommit(BatchSizeEnum batchSize)
    {
        var listBlockDescriptor = new FluentBlockSettingsDescriptor(BlockTypeEnum.List);
        listBlockDescriptor.ListUpdateMode = ListUpdateModeEnum.PeriodicBatchCommit;

        return listBlockDescriptor;
    }

    public IReprocessScopeDescriptor WithReprocessBatchCommitAtEnd()
    {
        var listBlockDescriptor = new FluentBlockSettingsDescriptor(BlockTypeEnum.List);
        listBlockDescriptor.ListUpdateMode = ListUpdateModeEnum.BatchCommitAtEnd;

        return listBlockDescriptor;
    }

    private List<string> Serialize(List<T> values)
    {
        var jsonValues = new List<string>();
        foreach (var value in values) jsonValues.Add(JsonGenericSerializer.Serialize(value));

        return jsonValues;
    }
}

public class FluentListBlockDescriptorBase<TItem, THeader> : IFluentListBlockDescriptorBase<TItem, THeader>
{
    public IOverrideConfigurationDescriptor WithSingleUnitCommit(List<TItem> values, THeader header, int maxBlockSize)
    {
        var jsonValues = Serialize(values);
        var jsonHeader = Serialize(header);

        var listBlockDescriptor = new FluentBlockSettingsDescriptor(jsonValues, jsonHeader, maxBlockSize);
        listBlockDescriptor.ListUpdateMode = ListUpdateModeEnum.SingleItemCommit;

        return listBlockDescriptor;
    }

    public IOverrideConfigurationDescriptor WithPeriodicCommit(List<TItem> values, THeader header, int maxBlockSize,
        BatchSizeEnum batchSize)
    {
        var jsonValues = Serialize(values);
        var jsonHeader = Serialize(header);
        var listBlockDescriptor = new FluentBlockSettingsDescriptor(jsonValues, jsonHeader, maxBlockSize);
        listBlockDescriptor.ListUpdateMode = ListUpdateModeEnum.PeriodicBatchCommit;

        switch (batchSize)
        {
            case BatchSizeEnum.NotSet:
                listBlockDescriptor.UncommittedItemsThreshold = 100;
                break;
            case BatchSizeEnum.Ten:
                listBlockDescriptor.UncommittedItemsThreshold = 10;
                break;
            case BatchSizeEnum.Fifty:
                listBlockDescriptor.UncommittedItemsThreshold = 50;
                break;
            case BatchSizeEnum.Hundred:
                listBlockDescriptor.UncommittedItemsThreshold = 100;
                break;
            case BatchSizeEnum.FiveHundred:
                listBlockDescriptor.UncommittedItemsThreshold = 500;
                break;
        }

        return listBlockDescriptor;
    }

    public IOverrideConfigurationDescriptor WithBatchCommitAtEnd(List<TItem> values, THeader header, int maxBlockSize)
    {
        var jsonValues = Serialize(values);
        var jsonHeader = Serialize(header);
        var listBlockDescriptor = new FluentBlockSettingsDescriptor(jsonValues, jsonHeader, maxBlockSize);
        listBlockDescriptor.ListUpdateMode = ListUpdateModeEnum.BatchCommitAtEnd;

        return listBlockDescriptor;
    }

    public IReprocessScopeDescriptor WithReprocessSingleUnitCommit()
    {
        var listBlockDescriptor = new FluentBlockSettingsDescriptor(BlockTypeEnum.List);
        listBlockDescriptor.ListUpdateMode = ListUpdateModeEnum.SingleItemCommit;

        return listBlockDescriptor;
    }

    public IReprocessScopeDescriptor WithReprocessPeriodicCommit(BatchSizeEnum batchSize)
    {
        var listBlockDescriptor = new FluentBlockSettingsDescriptor(BlockTypeEnum.List);
        listBlockDescriptor.ListUpdateMode = ListUpdateModeEnum.PeriodicBatchCommit;

        return listBlockDescriptor;
    }

    public IReprocessScopeDescriptor WithReprocessBatchCommitAtEnd()
    {
        var listBlockDescriptor = new FluentBlockSettingsDescriptor(BlockTypeEnum.List);
        listBlockDescriptor.ListUpdateMode = ListUpdateModeEnum.BatchCommitAtEnd;

        return listBlockDescriptor;
    }

    private List<string> Serialize(List<TItem> values)
    {
        var jsonValues = new List<string>();
        foreach (var value in values) jsonValues.Add(JsonGenericSerializer.Serialize(value));

        return jsonValues;
    }

    private string Serialize(THeader header)
    {
        return JsonGenericSerializer.Serialize(header);
    }
}