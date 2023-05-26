using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Taskling.Blocks.ListBlocks;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.Configuration;
using Taskling.Enums;
using Taskling.Fluent.ListBlocks;
using Taskling.Fluent.ObjectBlocks;
using Taskling.Fluent.RangeBlocks;
using Taskling.InfrastructureContracts;
using Taskling.Tasks;

namespace Taskling.Contexts;

public interface ITaskExecutionContext : IDisposable
{
    bool IsStarted { get; }

    IList<IDateRangeBlockContext> GetDateRangeBlocks(
        Func<IFluentDateRangeBlockDescriptor, object> fluentBlockRequest);

    void SetOptions(TaskId taskId,
        TaskExecutionOptions taskExecutionOptions, ITaskConfigurationRepository taskConfigurationRepository);

    bool TryStart();
    Task<bool> TryStartAsync();
    Task<bool> TryStartAsync(Guid referenceValue);
    Task<bool> TryStartAsync<TExecutionHeader>(TExecutionHeader executionHeader);
    Task<bool> TryStartAsync<TExecutionHeader>(TExecutionHeader executionHeader, Guid referenceValue);
    Task CompleteAsync();
    Task CheckpointAsync(string checkpointMessage);
    Task ErrorAsync(string errorMessage, bool treatTaskAsFailed);
    TExecutionHeader GetHeader<TExecutionHeader>();
    ICriticalSectionContext CreateCriticalSection();
    Task<IDateRangeBlock> GetLastDateRangeBlockAsync(LastBlockOrderEnum lastBlockOrder);
    Task<INumericRangeBlock> GetLastNumericRangeBlockAsync(LastBlockOrderEnum lastBlockOrder);
    INumericRangeBlock GetLastNumericRangeBlock(LastBlockOrderEnum lastBlockOrder);
    Task<IListBlock<T>> GetLastListBlockAsync<T>();
    Task<IListBlock<TItem, THeader>> GetLastListBlockAsync<TItem, THeader>();
    Task<IObjectBlock<T>> GetLastObjectBlockAsync<T>();

    Task<IList<IDateRangeBlockContext>> GetDateRangeBlocksAsync(
        Func<IFluentDateRangeBlockDescriptor, object> fluentBlockRequest);

    IList<INumericRangeBlockContext> GetNumericRangeBlocks(
        Func<IFluentNumericRangeBlockDescriptor, object> fluentBlockRequest);

    Task<IList<INumericRangeBlockContext>> GetNumericRangeBlocksAsync(
        Func<IFluentNumericRangeBlockDescriptor, object> fluentBlockRequest);

    Task<IList<IListBlockContext<T>>> GetListBlocksAsync<T>(
        Func<IFluentListBlockDescriptorBase<T>, object> fluentBlockRequest);

    Task<IList<IListBlockContext<TItem, THeader>>> GetListBlocksAsync<TItem, THeader>(
        Func<IFluentListBlockDescriptorBase<TItem, THeader>, object> fluentBlockRequest);

    Task<IList<IObjectBlockContext<T>>> GetObjectBlocksAsync<T>(
        Func<IFluentObjectBlockDescriptorBase<T>, object> fluentBlockRequest);

    Task<TaskExecutionMeta> GetLastExecutionMetaAsync();
    Task<IList<TaskExecutionMeta>> GetLastExecutionMetasAsync(int numberToRetrieve);
    Task<TaskExecutionMeta<TExecutionHeader>> GetLastExecutionMetaAsync<TExecutionHeader>();
    Task<IList<TaskExecutionMeta<TExecutionHeader>>> GetLastExecutionMetasAsync<TExecutionHeader>(int numberToRetrieve);
    IDateRangeBlock GetLastDateRangeBlock(LastBlockOrderEnum lastCreated);
    void Complete();
    void Error(string toString, bool b);

    IList<IListBlockContext<TItem, THeader>> GetListBlocks<TItem, THeader>(
        Func<IFluentListBlockDescriptorBase<TItem, THeader>, object> fluentBlockRequest);

    IListBlock<TItem, THeader> GetLastListBlock<TItem, THeader>();
}