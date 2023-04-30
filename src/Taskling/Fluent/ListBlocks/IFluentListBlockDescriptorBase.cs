using System.Collections.Generic;
using Taskling.Blocks.ListBlocks;

namespace Taskling.Fluent;

public interface IFluentListBlockDescriptorBase<T>
{
    IOverrideConfigurationDescriptor WithSingleUnitCommit(List<T> values, int maxBlockSize);
    IOverrideConfigurationDescriptor WithPeriodicCommit(List<T> values, int maxBlockSize, BatchSize batchSize);
    IOverrideConfigurationDescriptor WithBatchCommitAtEnd(List<T> values, int maxBlockSize);
    IReprocessScopeDescriptor ReprocessWithSingleUnitCommit();
    IReprocessScopeDescriptor ReprocessWithPeriodicCommit(BatchSize batchSize);
    IReprocessScopeDescriptor ReprocessWithBatchCommitAtEnd();
}

public interface IFluentListBlockDescriptorBase<TItem, THeader>
{
    IOverrideConfigurationDescriptor WithSingleUnitCommit(List<TItem> values, THeader header, int maxBlockSize);

    IOverrideConfigurationDescriptor WithPeriodicCommit(List<TItem> values, THeader header, int maxBlockSize,
        BatchSize batchSize);

    IOverrideConfigurationDescriptor WithBatchCommitAtEnd(List<TItem> values, THeader header, int maxBlockSize);
    IReprocessScopeDescriptor ReprocessWithSingleUnitCommit();
    IReprocessScopeDescriptor ReprocessWithPeriodicCommit(BatchSize batchSize);
    IReprocessScopeDescriptor ReprocessWithBatchCommitAtEnd();
}