using System;
using Taskling.Enums;

namespace Taskling.Fluent.RangeBlocks;

public class FluentRangeBlockDescriptor : IFluentDateRangeBlockDescriptor, IFluentNumericRangeBlockDescriptor
{
    public IReprocessScopeDescriptor WithReprocessDateRange()
    {
        return new FluentBlockSettingsDescriptor(BlockTypeEnum.DateRange);
    }

    public IOverrideConfigurationDescriptor WithRange(DateTime fromDate, DateTime toDate, TimeSpan maxBlockRange)
    {
        return new FluentBlockSettingsDescriptor(fromDate, toDate, maxBlockRange);
    }

    public IOverrideConfigurationDescriptor WithOnlyOldDateBlocks()
    {
        return new FluentBlockSettingsDescriptor(BlockTypeEnum.DateRange);
    }

    public IReprocessScopeDescriptor WithReprocessNumericRange()
    {
        return new FluentBlockSettingsDescriptor(BlockTypeEnum.NumericRange);
    }

    public IOverrideConfigurationDescriptor WithRange(long fromNumber, long toNumber, long maxBlockNumberRange)
    {
        return new FluentBlockSettingsDescriptor(fromNumber, toNumber, maxBlockNumberRange);
    }

    public IOverrideConfigurationDescriptor WithOnlyOldNumericBlocks()
    {
        return new FluentBlockSettingsDescriptor(BlockTypeEnum.NumericRange);
    }
}