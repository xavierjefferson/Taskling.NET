using System;
using System.Collections.Generic;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Tasks;

namespace Taskling.Fluent.Settings;

public interface IBlockSettings
{
    BlockType BlockType { get; set; }

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
    ListUpdateMode ListUpdateMode { get; set; }
    int UncommittedItemsThreshold { get; set; }

    // Reprocess Specific Task
    ReprocessOption ReprocessOption { get; set; }
    string ReferenceValueToReprocess { get; set; }

    // Configuration Overridable
    bool? MustReprocessFailedTasks { get; set; }
    TimeSpan? FailedTaskDetectionRange { get; set; }
    int? FailedTaskRetryLimit { get; set; }

    bool? MustReprocessDeadTasks { get; set; }
    TimeSpan? DeadTaskDetectionRange { get; set; }
    int? DeadTaskRetryLimit { get; set; }

    int? MaximumNumberOfBlocksLimit { get; set; }
}