using System;
using System.Collections.Generic;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Fluent.Settings;
using Taskling.Tasks;

namespace Taskling.Fluent;

public class FluentBlockSettingsDescriptor : IFluentBlockSettingsDescriptor, IOverrideConfigurationDescriptor,
    IReprocessScopeDescriptor, IReprocessTaskDescriptor, IBlockSettings, IComplete
{
    public FluentBlockSettingsDescriptor(BlockType blockType)
    {
        BlockType = blockType;
    }

    public FluentBlockSettingsDescriptor(DateTime fromDate, DateTime toDate, TimeSpan maxBlockRange)
    {
        FromDate = fromDate;
        ToDate = toDate;
        MaxBlockTimespan = maxBlockRange;
        BlockType = BlockType.DateRange;
    }

    public FluentBlockSettingsDescriptor(long fromNumber, long toNumber, long maxBlockRange)
    {
        FromNumber = fromNumber;
        ToNumber = toNumber;
        MaxBlockNumberRange = maxBlockRange;
        BlockType = BlockType.NumericRange;
    }

    public FluentBlockSettingsDescriptor(List<string> values, int maxBlockSize)
    {
        Values = values;
        MaxBlockSize = maxBlockSize;
        BlockType = BlockType.List;
    }

    public FluentBlockSettingsDescriptor(List<string> values, string header, int maxBlockSize)
    {
        Values = values;
        Header = header;
        MaxBlockSize = maxBlockSize;
        BlockType = BlockType.List;
    }

    public TaskDeathMode TaskDeathMode { get; set; }

    public bool? MustReprocessFailedTasks { get; set; }
    public TimeSpan? FailedTaskDetectionRange { get; set; }
    public int? FailedTaskRetryLimit { get; set; }

    public bool? MustReprocessDeadTasks { get; set; }
    public TimeSpan? DeadTaskDetectionRange { get; set; }
    public int? DeadTaskRetryLimit { get; set; }

    public int? MaximumNumberOfBlocksLimit { get; set; }
    public BlockType BlockType { get; set; }

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
    public ListUpdateMode ListUpdateMode { get; set; }
    public int UncommittedItemsThreshold { get; set; }

    // Reprocess Specific Task
    public ReprocessOption ReprocessOption { get; set; }
    public Guid ReferenceValueToReprocess { get; set; }

    public IFluentBlockSettingsDescriptor ReprocessFailedTasks(TimeSpan detectionRange, int retryLimit)
    {
        MustReprocessFailedTasks = true;
        FailedTaskDetectionRange = detectionRange;
        FailedTaskRetryLimit = retryLimit;
        return this;
    }

    public IFluentBlockSettingsDescriptor ReprocessDeadTasks(TimeSpan detectionRange, int retryLimit)
    {
        MustReprocessDeadTasks = true;
        DeadTaskDetectionRange = detectionRange;
        DeadTaskRetryLimit = retryLimit;
        return this;
    }

    public IComplete MaximumBlocksToGenerate(int maximumNumberOfBlocks)
    {
        MaximumNumberOfBlocksLimit = maximumNumberOfBlocks;
        return this;
    }

    public IFluentBlockSettingsDescriptor OverrideConfiguration()
    {
        return this;
    }

    public IReprocessTaskDescriptor AllBlocks()
    {
        ReprocessOption = ReprocessOption.Everything;
        return this;
    }

    public IReprocessTaskDescriptor PendingAndFailedBlocks()
    {
        ReprocessOption = ReprocessOption.PendingOrFailed;
        return this;
    }

    public IComplete OfExecutionWith(Guid referenceValue)
    {
        ReferenceValueToReprocess = referenceValue;
        return this;
    }
}