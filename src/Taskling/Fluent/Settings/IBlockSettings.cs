using System;
using System.Collections.Generic;
using Taskling.Enums;

namespace Taskling.Fluent.Settings;

public interface IBlockSettings
{
    BlockTypeEnum BlockType { get; set; }

    // DateRange
    DateTime? FromDate { get; set; }
    DateTime? ToDate { get; set; }
    TimeSpan? MaxBlockTimespan { get; set; }

    // NumericRange
    long? FromNumber { get; set; }
    long? ToNumber { get; set; }
    long? MaxBlockNumberRange { get; set; }

    // ListBlocks
    List<string> Values { get; set; }
    string Header { get; set; }
    int MaxBlockSize { get; set; }
    ListUpdateModeEnum ListUpdateMode { get; set; }
    int UncommittedItemsThreshold { get; set; }

    // Reprocess Specific Task
    ReprocessOptionEnum ReprocessOption { get; set; }
    Guid ReferenceValueToReprocess { get; set; }

    // Configuration Overridable
    bool? ReprocessFailedTasks { get; set; }
    TimeSpan? FailedTaskDetectionRange { get; set; }
    int? FailedTaskRetryLimit { get; set; }

    bool? ReprocessDeadTasks { get; set; }
    TimeSpan? DeadTaskDetectionRange { get; set; }
    int? DeadTaskRetryLimit { get; set; }

    int? MaxBlocksToGenerate { get; set; }
}