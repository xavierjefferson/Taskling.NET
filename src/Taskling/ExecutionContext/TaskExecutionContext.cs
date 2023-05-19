using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Nito.AsyncEx.Synchronous;
using Taskling.Blocks.Common;
using Taskling.Blocks.Factories;
using Taskling.Blocks.ListBlocks;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.Blocks.Requests;
using Taskling.CleanUp;
using Taskling.Configuration;
using Taskling.Contexts;
using Taskling.CriticalSection;
using Taskling.Exceptions;
using Taskling.Extensions;
using Taskling.Fluent;
using Taskling.Fluent.ListBlocks;
using Taskling.Fluent.ObjectBlocks;
using Taskling.Fluent.RangeBlocks;
using Taskling.Fluent.Settings;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Serialization;
using Taskling.Tasks;

namespace Taskling.ExecutionContext;

public class TaskExecutionContext : ITaskExecutionContext
{
    private const string NotActiveMessage =
        @"The context is not started. Only TryStart() can be called on a not started context. The context may not be in the started state because: 
1 - Completed() was already called,
2 - TryStart() returned false due to reaching the concurrency limit. When you call TryStart() always check the bool result and only continue when the result is true.
3 - the StartTask attribute has been used in PassThrough mode and the context could not start due to reaching the concurrency limit. When you use the StartTask attribute with PassThrough mode make sure you check the IsStarted property on the context before executing the logic of the job.
";

    private readonly IBlockFactory _blockFactory;
    private readonly ICleanUpService _cleanUpService;
    private readonly ICriticalSectionRepository _criticalSectionRepository;

    private readonly ILogger<TaskExecutionContext> _logger;
    private readonly ILoggerFactory _loggerFactory;

    private readonly IObjectBlockRepository _objectBlockRepository;
    private readonly IRangeBlockRepository _rangeBlockRepository;


    private readonly ITaskExecutionRepository _taskExecutionRepository;
    private readonly TasklingOptions _tasklingOptions;
    private ICriticalSectionContext _clientCriticalSectionContext;
    private bool _completeCalled;

    protected bool _disposed;
    private bool _executionHasFailed;
    private KeepAliveDaemon _keepAliveDaemon;
    private bool _startedCalled;
    private TaskConfiguration _taskConfiguration;

    private ITaskConfigurationRepository _taskConfigurationRepository;
    private object _taskExecutionHeader;

    private TaskExecutionInstance _taskExecutionInstance;
    private TaskExecutionOptions _taskExecutionOptions;
    private ICriticalSectionContext _userCriticalSectionContext;

    public TaskExecutionContext(ITaskExecutionRepository taskExecutionRepository,
        ICriticalSectionRepository criticalSectionRepository,
        IBlockFactory blockFactory,
        IRangeBlockRepository rangeBlockRepository, ILoggerFactory loggerFactory,
        IListBlockRepository listBlockRepository,
        IObjectBlockRepository objectBlockRepository, TasklingOptions tasklingOptions,
        ICleanUpService cleanUpService, ILogger<TaskExecutionContext> logger, IServiceProvider serviceProvider)
    {
        _logger = logger;
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _taskExecutionRepository =
            taskExecutionRepository ?? throw new ArgumentNullException(nameof(taskExecutionRepository));
        ;
        _criticalSectionRepository = criticalSectionRepository ??
                                     throw new ArgumentNullException(nameof(criticalSectionRepository));
        ;
        _blockFactory = blockFactory ?? throw new ArgumentNullException(nameof(blockFactory));
        ;
        _rangeBlockRepository = rangeBlockRepository ?? throw new ArgumentNullException(nameof(rangeBlockRepository));
        _loggerFactory = loggerFactory;

        ;
        _objectBlockRepository =
            objectBlockRepository ?? throw new ArgumentNullException(nameof(objectBlockRepository));
        _tasklingOptions = tasklingOptions;
        ;
        _cleanUpService = cleanUpService ?? throw new ArgumentNullException(nameof(cleanUpService));
    }


    private bool IsExecutionContextActive => _startedCalled && !_completeCalled;


    public IList<IDateRangeBlockContext> GetDateRangeBlocks(
        Func<IFluentDateRangeBlockDescriptor, object> fluentBlockRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return GetDateRangeBlocksAsync(fluentBlockRequest).WaitAndUnwrapException();
    }

    public bool IsStarted => IsExecutionContextActive;


    public void SetOptions(TaskId taskId,
        TaskExecutionOptions taskExecutionOptions, ITaskConfigurationRepository taskConfigurationRepository)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _taskConfigurationRepository = taskConfigurationRepository;
        _taskExecutionInstance = new TaskExecutionInstance(taskId);
        _taskExecutionOptions = taskExecutionOptions;
        _executionHasFailed = false;
        _taskConfiguration = taskConfigurationRepository.GetTaskConfiguration(taskId);
    }

    public bool TryStart()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return TryStartAsync().WaitAndUnwrapException();
    }

    public void Dispose()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));

        Dispose(true);
        GC.SuppressFinalize(this);
    }


    public async Task<bool> TryStartAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return await TryStartAsync(Guid.Empty).ConfigureAwait(false);
    }

    public async Task<bool> TryStartAsync(Guid referenceValue)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.LogDebug($"Entered {nameof(TryStartAsync)}");
        try
        {
            if (!_taskExecutionOptions.Enabled) return false;

            if (_startedCalled)
                throw new ExecutionException("The execution context has already been started");

            _startedCalled = true;

            CleanUpOldData();
            var startRequest = CreateStartRequest(referenceValue);

            try
            {
                var response = await _taskExecutionRepository.StartAsync(startRequest).ConfigureAwait(false);
                _taskExecutionInstance.TaskExecutionId = response.TaskExecutionId;
                _taskExecutionInstance.ExecutionTokenId = response.ExecutionTokenId;

                if (response.GrantStatus == GrantStatus.Denied)
                {
                    await CompleteAsync().ConfigureAwait(false);
                    return false;
                }

                if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
                    StartKeepAlive();
            }
            catch (Exception)
            {
                _completeCalled = true;
                throw;
            }


            return true;
        }
        finally
        {
            _logger.LogDebug($"Exiting {nameof(TryStartAsync)}");
        }
    }

    public async Task<bool> TryStartAsync<TExecutionHeader>(TExecutionHeader executionHeader)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _taskExecutionHeader = executionHeader;
        return await TryStartAsync().ConfigureAwait(false);
    }

    public async Task<bool> TryStartAsync<TExecutionHeader>(TExecutionHeader executionHeader, Guid referenceValue)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _taskExecutionHeader = executionHeader;
        return await TryStartAsync(referenceValue).ConfigureAwait(false);
    }

    public async Task CompleteAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.LogDebug($"Entered {nameof(CompleteAsync)}");
        try
        {
            if (IsExecutionContextActive)
            {
                _completeCalled = true;

                if (_keepAliveDaemon != null)
                    _keepAliveDaemon.Stop();

                var completeRequest = new TaskExecutionCompleteRequest(
                    _taskExecutionInstance.TaskId,
                    _taskExecutionInstance.TaskExecutionId,
                    _taskExecutionInstance.ExecutionTokenId);
                completeRequest.Failed = _executionHasFailed;


                var response = await _taskExecutionRepository.CompleteAsync(completeRequest).ConfigureAwait(false);
                _taskExecutionInstance.CompletedAt = response.CompletedAt;
            }
        }
        finally
        {
            _logger.LogDebug($"Exiting {nameof(CompleteAsync)}");
        }
    }

    public async Task CheckpointAsync(string checkpointMessage)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (!IsExecutionContextActive)
            throw new ExecutionException(NotActiveMessage);

        var request = new TaskExecutionCheckpointRequest(_taskExecutionInstance.TaskId)
        {
            TaskExecutionId = _taskExecutionInstance.TaskExecutionId,
            Message = checkpointMessage
        };
        await _taskExecutionRepository.CheckpointAsync(request).ConfigureAwait(false);
    }

    public async Task ErrorAsync(string errorMessage, bool treatTaskAsFailed)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (!IsExecutionContextActive)
            throw new ExecutionException(NotActiveMessage);

        _executionHasFailed = treatTaskAsFailed;

        var request = new TaskExecutionErrorRequest(_taskExecutionInstance.TaskId)
        {
            TaskExecutionId = _taskExecutionInstance.TaskExecutionId,
            Error = errorMessage,
            TreatTaskAsFailed = treatTaskAsFailed
        };
        await _taskExecutionRepository.ErrorAsync(request).ConfigureAwait(false);
    }

    public TExecutionHeader GetHeader<TExecutionHeader>()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (_taskExecutionHeader != null)
            return (TExecutionHeader)_taskExecutionHeader;

        return default;
    }

    public ICriticalSectionContext CreateCriticalSection()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (!IsExecutionContextActive)
            throw new ExecutionException(NotActiveMessage);

        if (IsUserCriticalSectionActive())
            throw new CriticalSectionException(
                "Only one user critical section context can be active at a time for one context. Check that you are not nesting critical sections with the same context.");

        _userCriticalSectionContext = new CriticalSectionContext(_criticalSectionRepository,
            _taskExecutionInstance,
            _taskExecutionOptions,
            CriticalSectionType.User, _tasklingOptions, _loggerFactory.CreateLogger<CriticalSectionContext>());

        return _userCriticalSectionContext;
    }

    public async Task<IList<IDateRangeBlockContext>> GetDateRangeBlocksAsync(
        Func<IFluentDateRangeBlockDescriptor, object> fluentBlockRequest)
    {
        _logger.Debug("ee614157-3097-4a5e-ac8e-3a63f107285b");
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return await GetBlocksAsync<IDateRangeBlockContext, FluentRangeBlockDescriptor, DateRangeBlockRequest,
            IBlockSettings>(BlockType.DateRange, fluentBlockRequest, ConvertToDateRangeBlockRequest, _blockFactory.GenerateDateRangeBlocksAsync);

        //if (!IsExecutionContextActive)
        //    throw new ExecutionException(NotActiveMessage);

        //var fluentDescriptor = fluentBlockRequest(new FluentRangeBlockDescriptor());
        //var settings = (IBlockSettings)fluentDescriptor;

        //var request = ConvertToDateRangeBlockRequest(settings);
        //if (ShouldProtect(request))
        //{
        //    var csContext = CreateClientCriticalSection();
        //    try
        //    {
        //        var csStarted = await csContext.TryStartAsync(new TimeSpan(0, 0, 20), 3).ConfigureAwait(false);
        //        if (csStarted)
        //            return await _blockFactory.GenerateDateRangeBlocksAsync(request).ConfigureAwait(false);

        //        throw new CriticalSectionException("Could not start a critical section in the alloted time");
        //    }
        //    finally
        //    {
        //        await csContext.CompleteAsync().ConfigureAwait(false);
        //    }
        //}

        //return await _blockFactory.GenerateDateRangeBlocksAsync(request).ConfigureAwait(false);
    }

    public IList<INumericRangeBlockContext> GetNumericRangeBlocks(
        Func<IFluentNumericRangeBlockDescriptor, object> fluentBlockRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return GetNumericRangeBlocksAsync(fluentBlockRequest).WaitAndUnwrapException();
    }

    public async Task<IList<INumericRangeBlockContext>> GetNumericRangeBlocksAsync(
        Func<IFluentNumericRangeBlockDescriptor, object> fluentBlockRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return await GetBlocksAsync<INumericRangeBlockContext, FluentRangeBlockDescriptor, NumericRangeBlockRequest,
            IBlockSettings>(BlockType.NumericRange, fluentBlockRequest, ConvertToNumericRangeBlockRequest, _blockFactory.GenerateNumericRangeBlocksAsync);

        //if (!IsExecutionContextActive)
        //    throw new ExecutionException(NotActiveMessage);

        //var fluentDescriptor = fluentBlockRequest(new FluentRangeBlockDescriptor());
        //var settings = (IBlockSettings)fluentDescriptor;

        //var request = ConvertToNumericRangeBlockRequest(settings);
        //if (ShouldProtect(request))
        //{
        //    var csContext = CreateClientCriticalSection();
        //    try
        //    {
        //        var csStarted = await csContext.TryStartAsync(new TimeSpan(0, 0, 20), 3).ConfigureAwait(false);
        //        if (csStarted)
        //            return await _blockFactory.GenerateNumericRangeBlocksAsync(request).ConfigureAwait(false);

        //        throw new CriticalSectionException("Could not start a critical section in the alloted time");
        //    }
        //    finally
        //    {
        //        await csContext.CompleteAsync().ConfigureAwait(false);
        //    }
        //}

        //return await _blockFactory.GenerateNumericRangeBlocksAsync(request).ConfigureAwait(false);
    }

    public async Task<IList<IListBlockContext<T>>> GetListBlocksAsync<T>(
        Func<IFluentListBlockDescriptorBase<T>, object> fluentBlockRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("1a79102d-7bbe-4da9-b2e7-b6d8d64c122e");
        return await GetBlocksAsync<IListBlockContext<T>, FluentListBlockDescriptorBase<T>, ListBlockRequest,
            IBlockSettings>(BlockType.List, fluentBlockRequest, GetListBlockRequest, _blockFactory.GenerateListBlocksAsync<T>);

        //if (!IsExecutionContextActive)
        //    throw new ExecutionException(NotActiveMessage);

        //var fluentDescriptor = fluentBlockRequest(new FluentListBlockDescriptorBase<T>());
        //var settings = (IBlockSettings)fluentDescriptor;

        //if (settings.BlockType == BlockType.List)
        //{
        //    var request = ConvertToListBlockRequest(settings);
        //    if (ShouldProtect(request))
        //    {
        //        var csContext = CreateClientCriticalSection();
        //        try
        //        {
        //            var csStarted = await csContext.TryStartAsync(new TimeSpan(0, 0, 20), 3).ConfigureAwait(false);
        //            if (csStarted)
        //                return await _blockFactory.GenerateListBlocksAsync<T>(request).ConfigureAwait(false);
        //            throw new CriticalSectionException("Could not start a critical section in the alloted time");
        //        }
        //        finally
        //        {
        //            await csContext.CompleteAsync().ConfigureAwait(false);
        //        }
        //    }

        //    return await _blockFactory.GenerateListBlocksAsync<T>(request).ConfigureAwait(false);
        //}

        //throw new NotSupportedException("BlockType not supported");
    }

    public async Task<IList<IListBlockContext<TItem, THeader>>> GetListBlocksAsync<TItem, THeader>(
        Func<IFluentListBlockDescriptorBase<TItem, THeader>, object> fluentBlockRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return await GetBlocksAsync<IListBlockContext<TItem, THeader>, FluentListBlockDescriptorBase<TItem, THeader>,
            ListBlockRequest, IBlockSettings>(BlockType.List, fluentBlockRequest, GetListBlockRequest, _blockFactory.GenerateListBlocksAsync<TItem, THeader>);
        //if (!IsExecutionContextActive)
        //    throw new ExecutionException(NotActiveMessage);

        //var fluentDescriptor = fluentBlockRequest(new FluentListBlockDescriptorBase<TItem, THeader>());
        //var settings = (IBlockSettings)fluentDescriptor;

        //if (settings.BlockType == BlockType.List)
        //{
        //    var request = ConvertToListBlockRequest(settings);
        //    if (ShouldProtect(request))
        //    {
        //        var csContext = CreateClientCriticalSection();
        //        try
        //        {
        //            var csStarted = await csContext.TryStartAsync(new TimeSpan(0, 0, 20), 3).ConfigureAwait(false);
        //            if (csStarted)
        //                return await _blockFactory.GenerateListBlocksAsync<TItem, THeader>(request)
        //                    .ConfigureAwait(false);
        //            throw new CriticalSectionException("Could not start a critical section in the alloted time");
        //        }
        //        finally
        //        {
        //            await csContext.CompleteAsync().ConfigureAwait(false);
        //        }
        //    }

        //    return await _blockFactory.GenerateListBlocksAsync<TItem, THeader>(request).ConfigureAwait(false);
        //}

        //throw new NotSupportedException("BlockType not supported");
    }

    public async Task<IList<IObjectBlockContext<T>>> GetObjectBlocksAsync<T>(
        Func<IFluentObjectBlockDescriptorBase<T>, object> fluentBlockRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return await GetBlocksAsync<IObjectBlockContext<T>, FluentObjectBlockDescriptorBase<T>, ObjectBlockRequest<T>,
            IObjectBlockSettings<T>>(BlockType.Object, fluentBlockRequest, GetObjectBlockRequest, _blockFactory.GenerateObjectBlocksAsync);
        //if (!IsExecutionContextActive)
        //    throw new ExecutionException(NotActiveMessage);

        //var fluentDescriptor = fluentBlockRequest(new FluentObjectBlockDescriptorBase<T>());
        //var settings = (IObjectBlockSettings<T>)fluentDescriptor;

        //var request = ConvertToObjectBlockRequest(settings);
        //if (ShouldProtect(request))
        //{
        //    var csContext = CreateClientCriticalSection();
        //    try
        //    {
        //        var csStarted = await csContext.TryStartAsync(new TimeSpan(0, 0, 20), 3).ConfigureAwait(false);
        //        if (csStarted)
        //            return await _blockFactory.GenerateObjectBlocksAsync(request).ConfigureAwait(false);
        //        throw new CriticalSectionException("Could not start a critical section in the alloted time");
        //    }
        //    finally
        //    {
        //        await csContext.CompleteAsync().ConfigureAwait(false);
        //    }
        //}

        //return await _blockFactory.GenerateObjectBlocksAsync(request).ConfigureAwait(false);
    }

    public async Task<IDateRangeBlock> GetLastDateRangeBlockAsync(LastBlockOrder lastBlockOrder)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (!IsExecutionContextActive)
            throw new ExecutionException(NotActiveMessage);

        var request = new LastBlockRequest(
            _taskExecutionInstance.TaskId,
            BlockType.DateRange);
        request.LastBlockOrder = lastBlockOrder;

        return await _rangeBlockRepository.GetLastRangeBlockAsync(request).ConfigureAwait(false);
    }

    public async Task<INumericRangeBlock> GetLastNumericRangeBlockAsync(LastBlockOrder lastBlockOrder)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (!IsExecutionContextActive)
            throw new ExecutionException(NotActiveMessage);

        var request = new LastBlockRequest(
            _taskExecutionInstance.TaskId,
            BlockType.NumericRange);
        request.LastBlockOrder = lastBlockOrder;

        return await _rangeBlockRepository.GetLastRangeBlockAsync(request).ConfigureAwait(false);
    }

    public INumericRangeBlock GetLastNumericRangeBlock(LastBlockOrder lastBlockOrder)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return GetLastNumericRangeBlockAsync(lastBlockOrder).WaitAndUnwrapException();
    }

    public async Task<IListBlock<T>> GetLastListBlockAsync<T>()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (!IsExecutionContextActive)
            throw new ExecutionException(NotActiveMessage);

        var request = new LastBlockRequest(
            _taskExecutionInstance.TaskId,
            BlockType.List);

        return await _blockFactory.GetLastListBlockAsync<T>(request).ConfigureAwait(false);
    }

    public async Task<IListBlock<TItem, THeader>> GetLastListBlockAsync<TItem, THeader>()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (!IsExecutionContextActive)
            throw new ExecutionException(NotActiveMessage);

        var request = new LastBlockRequest(
            _taskExecutionInstance.TaskId,
            BlockType.List);

        return await _blockFactory.GetLastListBlockAsync<TItem, THeader>(request).ConfigureAwait(false);
    }

    public async Task<IObjectBlock<T>> GetLastObjectBlockAsync<T>()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (!IsExecutionContextActive)
            throw new ExecutionException(NotActiveMessage);

        var request = new LastBlockRequest(
            _taskExecutionInstance.TaskId,
            BlockType.Object);

        return await _objectBlockRepository.GetLastObjectBlockAsync<T>(request).ConfigureAwait(false);
    }

    public async Task<TaskExecutionMeta> GetLastExecutionMetaAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var request = CreateTaskExecutionMetaRequest(1);

        var response = await _taskExecutionRepository.GetLastExecutionMetasAsync(request).ConfigureAwait(false);
        if (response.Executions != null && response.Executions.Any())
        {
            var meta = response.Executions.First();
            return new TaskExecutionMeta(meta.StartedAt, meta.CompletedAt, meta.Status, meta.ReferenceValue);
        }

        return null;
    }

    public async Task<IList<TaskExecutionMeta>> GetLastExecutionMetasAsync(int numberToRetrieve)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var request = CreateTaskExecutionMetaRequest(numberToRetrieve);

        var response = await _taskExecutionRepository.GetLastExecutionMetasAsync(request).ConfigureAwait(false);
        if (response.Executions != null && response.Executions.Any())
            return response.Executions
                .Select(x => new TaskExecutionMeta(x.StartedAt, x.CompletedAt, x.Status, x.ReferenceValue)).ToList();

        return new List<TaskExecutionMeta>();
    }

    public async Task<TaskExecutionMeta<TExecutionHeader>> GetLastExecutionMetaAsync<TExecutionHeader>()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var request = CreateTaskExecutionMetaRequest(1);

        var response = await _taskExecutionRepository.GetLastExecutionMetasAsync(request).ConfigureAwait(false);
        if (response.Executions != null && response.Executions.Any())
        {
            var meta = response.Executions.First();
            return new TaskExecutionMeta<TExecutionHeader>(meta.StartedAt,
                meta.CompletedAt,
                meta.Status,
                JsonGenericSerializer.Deserialize<TExecutionHeader>(meta.Header, true),
                meta.ReferenceValue);
        }

        return null;
    }

    public async Task<IList<TaskExecutionMeta<TExecutionHeader>>> GetLastExecutionMetasAsync<TExecutionHeader>(
        int numberToRetrieve)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var request = CreateTaskExecutionMetaRequest(numberToRetrieve);

        var response = await _taskExecutionRepository.GetLastExecutionMetasAsync(request).ConfigureAwait(false);
        if (response.Executions != null && response.Executions.Any())
            return response.Executions.Select(x => new TaskExecutionMeta<TExecutionHeader>(x.StartedAt,
                    x.CompletedAt,
                    x.Status,
                    JsonGenericSerializer.Deserialize<TExecutionHeader>(x.Header, true),
                    x.ReferenceValue))
                .ToList();

        return new List<TaskExecutionMeta<TExecutionHeader>>();
    }

    public IDateRangeBlock GetLastDateRangeBlock(LastBlockOrder lastCreated)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return GetLastDateRangeBlockAsync(lastCreated).WaitAndUnwrapException();
    }

    public void Complete()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        CompleteAsync().WaitAndUnwrapException();
    }

    public void Error(string toString, bool b)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        ErrorAsync(toString, b).WaitAndUnwrapException();
    }

    public IList<IListBlockContext<TItem, THeader>> GetListBlocks<TItem, THeader>(
        Func<IFluentListBlockDescriptorBase<TItem, THeader>, object> fluentBlockRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return GetListBlocksAsync(fluentBlockRequest).WaitAndUnwrapException();
    }

    public IListBlock<TItem, THeader> GetLastListBlock<TItem, THeader>()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return GetLastListBlockAsync<TItem, THeader>().WaitAndUnwrapException();
    }

    ~TaskExecutionContext()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        Dispose(false);
    }

    protected virtual void Dispose(bool disposing)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (_disposed)
            return;

        if (disposing)
        {
        }

        if (_startedCalled && !_completeCalled) Task.Run(async () => await CompleteAsync().ConfigureAwait(false));

        _disposed = true;
    }

    public IList<IDateRangeBlockContext> GetDateRangeBlocksc(
        Func<IFluentDateRangeBlockDescriptor, object> fluentBlockRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return GetDateRangeBlocksAsync(fluentBlockRequest).WaitAndUnwrapException();
    }

    public async Task<IList<T>> GetBlocksAsync<T, U, V, W>(BlockType blockType, Func<U, object> fluentBlockRequest,
        Func<W, V> createRequestFunc,
        Func<V, Task<IList<T>>> generateFunc)
        where V : BlockRequest where U : new() where W : IBlockSettings
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.LogDebug($"Entering block request for {typeof(T).Name}");
        try
        {
            if (!IsExecutionContextActive)
            {
                _logger.Debug("1aeb9ff0-5de1-4141-8bf6-8d1522c2036a");
                throw new ExecutionException(NotActiveMessage);

            }
            _logger.Debug("ac3a715a-7ce4-4a38-bee6-d2563aa92bfc");
            var fluentDescriptor = fluentBlockRequest(new U());
            var settings = (W)fluentDescriptor;
            if (settings.BlockType == blockType)
            {
                var request = createRequestFunc(settings);
                if (ShouldProtect(request))
                {
                    _logger.Debug("84cd0420-e6b3-4d1e-8c44-4b3b274ef885");
                    _logger.LogDebug("Request is protected - creating critical section");
                    var csContext = CreateClientCriticalSection();
                    try
                    {
                        var csStarted = await csContext.TryStartAsync(_tasklingOptions.CriticalSectionRetry,
                            _tasklingOptions.CriticalSectionAttemptCount).ConfigureAwait(false);
                        if (csStarted)
                        {
                            _logger.Debug("7fcac822-d753-43f4-8a53-f16eb0e0f7c9");
                            return await generateFunc(request).ConfigureAwait(false);

                        }

                        _logger.Debug("3081d755-b7ec-4858-bbfc-9cd879efd3df");
                        throw new CriticalSectionException("Could not start a critical section in the alloted time");
                    }
                    finally
                    {
                        await csContext.CompleteAsync().ConfigureAwait(false);
                    }
                }
                else
                {
                    _logger.Debug("5dc2f323-1101-4db5-a487-55bff6f49bb9");
                }

                _logger.LogDebug("Request is unprotected");
                return await generateFunc(request).ConfigureAwait(false);
            }
            throw new NotSupportedException("BlockType not supported");
        }
        finally
        {
            _logger.LogDebug($"Exiting block request for {typeof(T).Name}");
        }
    }

    private void CleanUpOldData()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _cleanUpService.CleanOldData(_taskExecutionInstance.TaskId,
            _taskExecutionInstance.TaskExecutionId, _taskConfigurationRepository);
    }

    private TaskExecutionStartRequest CreateStartRequest(Guid referenceValue)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("d117a96a-84e3-4974-97ef-d353e3f46636");
        var startRequest = new TaskExecutionStartRequest(
            _taskExecutionInstance.TaskId,
            _taskExecutionOptions.TaskDeathMode,
            _taskExecutionOptions.ConcurrencyLimit,
            _taskConfiguration.FailedTaskRetryLimit,
            _taskConfiguration.DeadTaskRetryLimit);
        _logger.Debug($"{nameof(startRequest)}={JsonConvert.SerializeObject(startRequest, Formatting.Indented)}");

        SetStartRequestValues(startRequest, referenceValue);
        SetStartRequestTasklingVersion(startRequest);
        SerializeHeaderIfExists(startRequest);

        return startRequest;
    }

    private void SetStartRequestTasklingVersion(TaskExecutionStartRequest startRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("46b5407c-94ea-4a82-854b-2e4c1d2f7873");
        var assembly = Assembly.GetExecutingAssembly();
        var fileVersionInfo = FileVersionInfo.GetVersionInfo(assembly.Location);
        var version = fileVersionInfo.ProductVersion;
        startRequest.TasklingVersion = version;
    }

    private void SetStartRequestValues(TaskExecutionStartRequest startRequest, Guid referenceValue)
    {
        _logger.Debug("edbf81ac-5456-4a0d-a36b-f1732b87e8b6");
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
        {
            if (!_taskExecutionOptions.KeepAliveInterval.HasValue)
                throw new ExecutionArgumentsException("KeepAliveInterval must be set when using KeepAlive mode");

            if (!_taskExecutionOptions.KeepAliveDeathThreshold.HasValue)
                throw new ExecutionArgumentsException("KeepAliveDeathThreshold must be set when using KeepAlive mode");


            startRequest.KeepAliveDeathThreshold = _taskExecutionOptions.KeepAliveDeathThreshold;
            startRequest.KeepAliveInterval = _taskExecutionOptions.KeepAliveInterval;
        }

        if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.Override)
        {
            if (!_taskExecutionOptions.OverrideThreshold.HasValue)
                throw new ExecutionArgumentsException("OverrideThreshold must be set when using KeepAlive mode");

            startRequest.OverrideThreshold = _taskExecutionOptions.OverrideThreshold.Value;
        }

        startRequest.ReferenceValue = referenceValue;
    }

    private void SerializeHeaderIfExists(TaskExecutionStartRequest startRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (_taskExecutionHeader != null)
        {
            _logger.Debug("3d5600a8-8718-4de7-8549-9c8cb369929f");
            startRequest.TaskExecutionHeader = JsonGenericSerializer.Serialize(_taskExecutionHeader);
        }
        else
        {
            _logger.Debug("166166b3-dd14-4dda-a7e3-7132bd600984");
        }
    }

    private void StartKeepAlive()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var keepAliveRequest = new SendKeepAliveRequest(_taskExecutionInstance.TaskId)
        {
            TaskExecutionId = _taskExecutionInstance.TaskExecutionId,
            ExecutionTokenId = _taskExecutionInstance.ExecutionTokenId
        };

        _keepAliveDaemon = new KeepAliveDaemon(_taskExecutionRepository, new WeakReference(this),
            _loggerFactory.CreateLogger<KeepAliveDaemon>());
        _keepAliveDaemon.Run(keepAliveRequest, _taskExecutionOptions.KeepAliveInterval.Value);
    }

    private DateRangeBlockRequest ConvertToDateRangeBlockRequest(IBlockSettings settings)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var request = new DateRangeBlockRequest(_taskExecutionInstance.TaskId);
        request.TaskExecutionId = _taskExecutionInstance.TaskExecutionId;
        request.TaskDeathMode = _taskExecutionOptions.TaskDeathMode;

        if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
            request.KeepAliveDeathThreshold = _taskExecutionOptions.KeepAliveDeathThreshold.Value;
        else
            request.OverrideDeathThreshold = _taskExecutionOptions.OverrideThreshold.Value;

        request.RangeBegin = settings.FromDate;
        request.RangeEnd = settings.ToDate;
        request.MaxBlockRange = settings.MaxBlockTimespan;
        request.ReprocessReferenceValue = settings.ReferenceValueToReprocess;
        request.ReprocessOption = settings.ReprocessOption;

        SetConfigurationOverridableSettings(request, settings);

        return request;
    }

    private NumericRangeBlockRequest ConvertToNumericRangeBlockRequest(IBlockSettings settings)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var request = new NumericRangeBlockRequest(_taskExecutionInstance.TaskId);
        request.TaskExecutionId = _taskExecutionInstance.TaskExecutionId;
        request.TaskDeathMode = _taskExecutionOptions.TaskDeathMode;

        if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
            request.KeepAliveDeathThreshold = _taskExecutionOptions.KeepAliveDeathThreshold.Value;
        else
            request.OverrideDeathThreshold = _taskExecutionOptions.OverrideThreshold.Value;

        request.RangeBegin = settings.FromNumber;
        request.RangeEnd = settings.ToNumber;
        request.BlockSize = settings.MaxBlockNumberRange;
        request.ReprocessReferenceValue = settings.ReferenceValueToReprocess;
        request.ReprocessOption = settings.ReprocessOption;

        SetConfigurationOverridableSettings(request, settings);

        return request;
    }

    private ListBlockRequest GetListBlockRequest(IBlockSettings settings)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var request = new ListBlockRequest(_taskExecutionInstance.TaskId);


        request.TaskExecutionId = _taskExecutionInstance.TaskExecutionId;
        request.TaskDeathMode = _taskExecutionOptions.TaskDeathMode;

        if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
            request.KeepAliveDeathThreshold = _taskExecutionOptions.KeepAliveDeathThreshold.Value;
        else
            request.OverrideDeathThreshold = _taskExecutionOptions.OverrideThreshold.Value;

        request.SerializedValues = settings.Values;
        request.SerializedHeader = settings.Header;
        request.CompressionThreshold = _taskConfiguration.MaxLengthForNonCompressedData;
        request.MaxStatusReasonLength = _taskConfiguration.MaxStatusReason;

        request.MaxBlockSize = settings.MaxBlockSize;
        request.ListUpdateMode = settings.ListUpdateMode;
        request.UncommittedItemsThreshold = settings.UncommittedItemsThreshold;

        request.ReprocessReferenceValue = settings.ReferenceValueToReprocess;
        request.ReprocessOption = settings.ReprocessOption;

        SetConfigurationOverridableSettings(request, settings);


        return request;
    }

    private ObjectBlockRequest<T> GetObjectBlockRequest<T>(IObjectBlockSettings<T> settings)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var request = new ObjectBlockRequest<T>(settings.Object,
            _taskConfiguration.MaxLengthForNonCompressedData, _taskExecutionInstance.TaskId);

        request.TaskExecutionId = _taskExecutionInstance.TaskExecutionId;
        request.TaskDeathMode = _taskExecutionOptions.TaskDeathMode;

        if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
            request.KeepAliveDeathThreshold = _taskExecutionOptions.KeepAliveDeathThreshold.Value;
        else
            request.OverrideDeathThreshold = _taskExecutionOptions.OverrideThreshold.Value;

        request.ReprocessReferenceValue = settings.ReferenceValueToReprocess;
        request.ReprocessOption = settings.ReprocessOption;

        SetConfigurationOverridableSettings(request, settings);

        return request;
    }

    private void SetConfigurationOverridableSettings(BlockRequest request, IBlockSettings settings)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        request.ReprocessDeadTasks = settings.MustReprocessDeadTasks ?? _taskConfiguration.ReprocessDeadTasks;

        request.ReprocessFailedTasks = settings.MustReprocessFailedTasks ?? _taskConfiguration.ReprocessFailedTasks;

        request.DeadTaskRetryLimit = settings.DeadTaskRetryLimit ?? _taskConfiguration.DeadTaskRetryLimit;

        request.FailedTaskRetryLimit = settings.FailedTaskRetryLimit ?? _taskConfiguration.FailedTaskRetryLimit;

        if (request.ReprocessDeadTasks)
            request.DeadTaskDetectionRange =
                settings.DeadTaskDetectionRange ?? _taskConfiguration.ReprocessDeadTasksDetectionRange;

        if (request.ReprocessFailedTasks)
            request.FailedTaskDetectionRange = settings.FailedTaskDetectionRange ??
                                               _taskConfiguration.ReprocessFailedTasksDetectionRange;

        request.MaxBlocks = settings.MaximumNumberOfBlocksLimit ?? _taskConfiguration.MaxBlocksToGenerate;
    }

    private bool IsUserCriticalSectionActive()
    {
        _logger.Debug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("dc73085c-0af9-4f3f-b736-9064dde835c3");
        var tmp = _userCriticalSectionContext != null && _userCriticalSectionContext.IsActive();
        _logger.Debug($"{nameof(IsUserCriticalSectionActive)} = {tmp}");
        return tmp;
    }

    private bool ShouldProtect(BlockRequest blockRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug(JsonConvert.SerializeObject(blockRequest, Formatting.Indented));
        _logger.Debug("254f3474-fc1a-45c6-9836-66864ac3bc02");
        var tmp = (blockRequest.ReprocessDeadTasks || blockRequest.ReprocessFailedTasks) && !IsUserCriticalSectionActive();
        _logger.Debug(tmp.ToString());
        return tmp;
    }

    private ICriticalSectionContext CreateClientCriticalSection()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (IsClientCriticalSectionActive())
            throw new CriticalSectionException("Only one client critical section context can be active at a time");

        _clientCriticalSectionContext = new CriticalSectionContext(_criticalSectionRepository,
            _taskExecutionInstance,
            _taskExecutionOptions,
            CriticalSectionType.Client, _tasklingOptions, _loggerFactory.CreateLogger<CriticalSectionContext>());

        return _clientCriticalSectionContext;
    }

    private bool IsClientCriticalSectionActive()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return _clientCriticalSectionContext != null && _clientCriticalSectionContext.IsActive();
    }

    private TaskExecutionMetaRequest CreateTaskExecutionMetaRequest(int numberToRetrieve)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var request = new TaskExecutionMetaRequest(_taskExecutionInstance.TaskId);

        request.ExecutionsToRetrieve = numberToRetrieve;

        return request;
    }
}