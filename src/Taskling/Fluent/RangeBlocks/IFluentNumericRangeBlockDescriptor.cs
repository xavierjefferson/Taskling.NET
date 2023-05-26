namespace Taskling.Fluent.RangeBlocks;

public interface IFluentNumericRangeBlockDescriptor
{
    IOverrideConfigurationDescriptor WithRange(long fromNumber, long toNumber, long maxBlockNumberRange);
    IOverrideConfigurationDescriptor WithOnlyOldNumericBlocks();
    IReprocessScopeDescriptor WithReprocessNumericRange();
}