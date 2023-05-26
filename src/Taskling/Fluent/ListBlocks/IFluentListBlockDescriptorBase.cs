using System.Collections.Generic;
using Taskling.Enums;

namespace Taskling.Fluent.ListBlocks;

public interface IFluentListBlockDescriptorBase<T>
{
    IOverrideConfigurationDescriptor WithSingleUnitCommit(List<T> values, int maxBlockSize);
    IOverrideConfigurationDescriptor WithPeriodicCommit(List<T> values, int maxBlockSize, BatchSizeEnum batchSize);
    IOverrideConfigurationDescriptor WithBatchCommitAtEnd(List<T> values, int maxBlockSize);
    IReprocessScopeDescriptor WithReprocessSingleUnitCommit();
    IReprocessScopeDescriptor WithReprocessPeriodicCommit(BatchSizeEnum batchSize);
    IReprocessScopeDescriptor WithReprocessBatchCommitAtEnd();
}

public interface IFluentListBlockDescriptorBase<TItem, THeader>
{
    IOverrideConfigurationDescriptor WithSingleUnitCommit(List<TItem> values, THeader header, int maxBlockSize);

    IOverrideConfigurationDescriptor WithPeriodicCommit(List<TItem> values, THeader header, int maxBlockSize,
        BatchSizeEnum batchSize);

    IOverrideConfigurationDescriptor WithBatchCommitAtEnd(List<TItem> values, THeader header, int maxBlockSize);
    IReprocessScopeDescriptor WithReprocessSingleUnitCommit();
    IReprocessScopeDescriptor WithReprocessPeriodicCommit(BatchSizeEnum batchSize);
    IReprocessScopeDescriptor WithReprocessBatchCommitAtEnd();
}