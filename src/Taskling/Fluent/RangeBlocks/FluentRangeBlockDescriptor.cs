using System;
using Taskling.Blocks.Common;

namespace Taskling.Fluent.RangeBlocks;

public class FluentRangeBlockDescriptor : IFluentDateRangeBlockDescriptor, IFluentNumericRangeBlockDescriptor
{
    public IReprocessScopeDescriptor ReprocessDateRange()
    {
        return new FluentBlockSettingsDescriptor(BlockType.DateRange);
    }

    public IOverrideConfigurationDescriptor WithRange(DateTime fromDate, DateTime toDate, TimeSpan maxBlockRange)
    {
        return new FluentBlockSettingsDescriptor(fromDate, toDate, maxBlockRange);
    }

    public IOverrideConfigurationDescriptor OnlyOldDateBlocks()
    {
        return new FluentBlockSettingsDescriptor(BlockType.DateRange);
    }

    public IReprocessScopeDescriptor ReprocessNumericRange()
    {
        return new FluentBlockSettingsDescriptor(BlockType.NumericRange);
    }

    public IOverrideConfigurationDescriptor WithRange(long fromNumber, long toNumber, long maxBlockNumberRange)
    {
        return new FluentBlockSettingsDescriptor(fromNumber, toNumber, maxBlockNumberRange);
    }

    public IOverrideConfigurationDescriptor OnlyOldNumericBlocks()
    {
        return new FluentBlockSettingsDescriptor(BlockType.NumericRange);
    }
}