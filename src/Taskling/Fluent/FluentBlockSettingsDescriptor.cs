using System;
using System.Collections.Generic;
using Taskling.Enums;
using Taskling.Fluent.Settings;

namespace Taskling.Fluent;

public class FluentBlockSettingsDescriptor : IFluentBlockSettingsDescriptor, IOverrideConfigurationDescriptor,
    IReprocessScopeDescriptor, IReprocessTaskDescriptor, IBlockSettings, IComplete
{
    public FluentBlockSettingsDescriptor(BlockTypeEnum blockType)
    {
        BlockType = blockType;
    }

    public FluentBlockSettingsDescriptor(DateTime fromDate, DateTime toDate, TimeSpan maxBlockRange)
    {
        FromDate = fromDate;
        ToDate = toDate;
        MaxBlockTimespan = maxBlockRange;
        BlockType = BlockTypeEnum.DateRange;
    }

    public FluentBlockSettingsDescriptor(long fromNumber, long toNumber, long maxBlockRange)
    {
        FromNumber = fromNumber;
        ToNumber = toNumber;
        MaxBlockNumberRange = maxBlockRange;
        BlockType = BlockTypeEnum.NumericRange;
    }

    public FluentBlockSettingsDescriptor(List<string> values, int maxBlockSize)
    {
        Values = values;
        MaxBlockSize = maxBlockSize;
        BlockType = BlockTypeEnum.List;
    }

    public FluentBlockSettingsDescriptor(List<string> values, string header, int maxBlockSize)
    {
        Values = values;
        Header = header;
        MaxBlockSize = maxBlockSize;
        BlockType = BlockTypeEnum.List;
    }

    public TaskDeathModeEnum TaskDeathMode { get; set; }

    public bool? ReprocessFailedTasks { get; set; }
    public TimeSpan? FailedTaskDetectionRange { get; set; }
    public int? FailedTaskRetryLimit { get; set; }

    public bool? ReprocessDeadTasks { get; set; }
    public TimeSpan? DeadTaskDetectionRange { get; set; }
    public int? DeadTaskRetryLimit { get; set; }

    public int? MaxBlocksToGenerate { get; set; }
    public BlockTypeEnum BlockType { get; set; }

    // Date Range
    public DateTime? FromDate { get; set; }
    public DateTime? ToDate { get; set; }
    public TimeSpan? MaxBlockTimespan { get; set; }

    // Numeric Range
    public long? FromNumber { get; set; }
    public long? ToNumber { get; set; }
    public long? MaxBlockNumberRange { get; set; }

    // ListBlocks
    public List<string> Values { get; set; }
    public string Header { get; set; }
    public int MaxBlockSize { get; set; }
    public ListUpdateModeEnum ListUpdateMode { get; set; }
    public int UncommittedItemsThreshold { get; set; }

    // Reprocess Specific Task
    public ReprocessOptionEnum ReprocessOption { get; set; }
    public Guid ReferenceValueToReprocess { get; set; }

    public IFluentBlockSettingsDescriptor WithReprocessFailedTasks(TimeSpan detectionRange, int retryLimit)
    {
        ReprocessFailedTasks = true;
        FailedTaskDetectionRange = detectionRange;
        FailedTaskRetryLimit = retryLimit;
        return this;
    }

    public IFluentBlockSettingsDescriptor WithReprocessDeadTasks(TimeSpan detectionRange, int retryLimit)
    {
        ReprocessDeadTasks = true;
        DeadTaskDetectionRange = detectionRange;
        DeadTaskRetryLimit = retryLimit;
        return this;
    }

    public IComplete WithMaximumBlocksToGenerate(int maximumNumberOfBlocks)
    {
        MaxBlocksToGenerate = maximumNumberOfBlocks;
        return this;
    }

    public IFluentBlockSettingsDescriptor OverrideConfiguration()
    {
        return this;
    }

    public IReprocessTaskDescriptor AllBlocks()
    {
        ReprocessOption = ReprocessOptionEnum.Everything;
        return this;
    }

    public IReprocessTaskDescriptor PendingAndFailedBlocks()
    {
        ReprocessOption = ReprocessOptionEnum.PendingOrFailed;
        return this;
    }

    public IComplete OfExecutionWith(Guid referenceValue)
    {
        ReferenceValueToReprocess = referenceValue;
        return this;
    }
}