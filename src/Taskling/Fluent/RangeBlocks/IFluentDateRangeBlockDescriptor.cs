﻿using System;

namespace Taskling.Fluent.RangeBlocks;

public interface IFluentDateRangeBlockDescriptor
{
    IOverrideConfigurationDescriptor WithRange(DateTime fromDate, DateTime toDate, TimeSpan maxBlockRange);
    IOverrideConfigurationDescriptor OnlyOldDateBlocks();
    IReprocessScopeDescriptor ReprocessDateRange();
}