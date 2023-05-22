using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
    private readonly StartupOptions _startupOptions;
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
        IObjectBlockRepository objectBlockRepository, StartupOptions startupOptions,
        ICleanUpService cleanUpService, ILogger<TaskExecutionContext> logger, IServiceProvider serviceProvider)
    {
        _logger = logger;
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
        _startupOptions = startupOptions;
        ;
        _cleanUpService = cleanUpService ?? throw new ArgumentNullException(nameof(cleanUpService));
    }


    private bool IsExecutionContextActive => _startedCalled && !_completeCalled;


    public IList<IDateRangeBlockContext> GetDateRangeBlocks(
        Func<IFluentDateRangeBlockDescriptor, object> fluentBlockRequest)
    {
        return GetDateRangeBlocksAsync(fluentBlockRequest).WaitAndUnwrapException();
    }

    public bool IsStarted => IsExecutionContextActive;


    public void SetOptions(TaskId taskId,
        TaskExecutionOptions taskExecutionOptions, ITaskConfigurationRepository taskConfigurationRepository)
    {
        _taskConfigurationRepository = taskConfigurationRepository;
        _taskExecutionInstance = new TaskExecutionInstance(taskId);
        _taskExecutionOptions = taskExecutionOptions;
        _executionHasFailed = false;
        _taskConfiguration = taskConfigurationRepository.GetTaskConfiguration(taskId);
    }

    public bool TryStart()
    {
        return TryStartAsync().WaitAndUnwrapException();
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }


    public async Task<bool> TryStartAsync()
    {
        return await TryStartAsync(Guid.Empty).ConfigureAwait(false);
    }

    public async Task<bool> TryStartAsync(Guid referenceValue)
    {
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
        _taskExecutionHeader = executionHeader;
        return await TryStartAsync().ConfigureAwait(false);
    }

    public async Task<bool> TryStartAsync<TExecutionHeader>(TExecutionHeader executionHeader, Guid referenceValue)
    {
        _taskExecutionHeader = executionHeader;
        return await TryStartAsync(referenceValue).ConfigureAwait(false);
    }

    public async Task CompleteAsync()
    {
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
        if (_taskExecutionHeader != null)
            return (TExecutionHeader)_taskExecutionHeader;

        return default;
    }

    public ICriticalSectionContext CreateCriticalSection()
    {
        if (!IsExecutionContextActive)
            throw new ExecutionException(NotActiveMessage);

        if (IsUserCriticalSectionActive())
            throw new CriticalSectionException(
                "Only one user critical section context can be active at a time for one context. Check that you are not nesting critical sections with the same context.");

        _userCriticalSectionContext = new CriticalSectionContext(_criticalSectionRepository,
            _taskExecutionInstance,
            _taskExecutionOptions,
            CriticalSectionType.User, _startupOptions, _loggerFactory.CreateLogger<CriticalSectionContext>());

        return _userCriticalSectionContext;
    }

    public async Task<IList<IDateRangeBlockContext>> GetDateRangeBlocksAsync(
        Func<IFluentDateRangeBlockDescriptor, object> fluentBlockRequest)
    {
        return await GetBlocksAsync<IDateRangeBlockContext, FluentRangeBlockDescriptor, DateRangeBlockRequest,
            IBlockSettings>(BlockType.DateRange, fluentBlockRequest, GetDateRangeBlockRequest,
            _blockFactory.GenerateDateRangeBlocksAsync);
    }

    public IList<INumericRangeBlockContext> GetNumericRangeBlocks(
        Func<IFluentNumericRangeBlockDescriptor, object> fluentBlockRequest)
    {
        return GetNumericRangeBlocksAsync(fluentBlockRequest).WaitAndUnwrapException();
    }

    public async Task<IList<INumericRangeBlockContext>> GetNumericRangeBlocksAsync(
        Func<IFluentNumericRangeBlockDescriptor, object> fluentBlockRequest)
    {
        return await GetBlocksAsync<INumericRangeBlockContext, FluentRangeBlockDescriptor, NumericRangeBlockRequest,
            IBlockSettings>(BlockType.NumericRange, fluentBlockRequest, GetNumericRangeBlockRequest,
            _blockFactory.GenerateNumericRangeBlocksAsync);
    }

    public async Task<IList<IListBlockContext<T>>> GetListBlocksAsync<T>(
        Func<IFluentListBlockDescriptorBase<T>, object> fluentBlockRequest)
    {
        return await GetBlocksAsync<IListBlockContext<T>, FluentListBlockDescriptorBase<T>, ListBlockRequest,
            IBlockSettings>(BlockType.List, fluentBlockRequest, GetListBlockRequest,
            _blockFactory.GenerateListBlocksAsync<T>);
    }

    public async Task<IList<IListBlockContext<TItem, THeader>>> GetListBlocksAsync<TItem, THeader>(
        Func<IFluentListBlockDescriptorBase<TItem, THeader>, object> fluentBlockRequest)
    {
        return await GetBlocksAsync<IListBlockContext<TItem, THeader>, FluentListBlockDescriptorBase<TItem, THeader>,
            ListBlockRequest, IBlockSettings>(BlockType.List, fluentBlockRequest, GetListBlockRequest,
            _blockFactory.GenerateListBlocksAsync<TItem, THeader>);
    }

    public async Task<IList<IObjectBlockContext<T>>> GetObjectBlocksAsync<T>(
        Func<IFluentObjectBlockDescriptorBase<T>, object> fluentBlockRequest)
    {
        return await GetBlocksAsync<IObjectBlockContext<T>, FluentObjectBlockDescriptorBase<T>, ObjectBlockRequest<T>,
            IObjectBlockSettings<T>>(BlockType.Object, fluentBlockRequest, GetObjectBlockRequest,
            _blockFactory.GenerateObjectBlocksAsync);
    }

    public async Task<IDateRangeBlock> GetLastDateRangeBlockAsync(LastBlockOrder lastBlockOrder)
    {
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
        return GetLastNumericRangeBlockAsync(lastBlockOrder).WaitAndUnwrapException();
    }

    public async Task<IListBlock<T>> GetLastListBlockAsync<T>()
    {
        if (!IsExecutionContextActive)
            throw new ExecutionException(NotActiveMessage);

        var request = new LastBlockRequest(
            _taskExecutionInstance.TaskId,
            BlockType.List);

        return await _blockFactory.GetLastListBlockAsync<T>(request).ConfigureAwait(false);
    }

    public async Task<IListBlock<TItem, THeader>> GetLastListBlockAsync<TItem, THeader>()
    {
        if (!IsExecutionContextActive)
            throw new ExecutionException(NotActiveMessage);

        var request = new LastBlockRequest(
            _taskExecutionInstance.TaskId,
            BlockType.List);

        return await _blockFactory.GetLastListBlockAsync<TItem, THeader>(request).ConfigureAwait(false);
    }

    public async Task<IObjectBlock<T>> GetLastObjectBlockAsync<T>()
    {
        if (!IsExecutionContextActive)
            throw new ExecutionException(NotActiveMessage);

        var request = new LastBlockRequest(
            _taskExecutionInstance.TaskId,
            BlockType.Object);

        return await _objectBlockRepository.GetLastObjectBlockAsync<T>(request).ConfigureAwait(false);
    }

    public async Task<TaskExecutionMeta> GetLastExecutionMetaAsync()
    {
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
        var request = CreateTaskExecutionMetaRequest(numberToRetrieve);

        var response = await _taskExecutionRepository.GetLastExecutionMetasAsync(request).ConfigureAwait(false);
        if (response.Executions != null && response.Executions.Any())
            return response.Executions
                .Select(x => new TaskExecutionMeta(x.StartedAt, x.CompletedAt, x.Status, x.ReferenceValue)).ToList();

        return new List<TaskExecutionMeta>();
    }

    public async Task<TaskExecutionMeta<TExecutionHeader>> GetLastExecutionMetaAsync<TExecutionHeader>()
    {
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
        return GetLastDateRangeBlockAsync(lastCreated).WaitAndUnwrapException();
    }

    public void Complete()
    {
        CompleteAsync().WaitAndUnwrapException();
    }

    public void Error(string toString, bool b)
    {
        ErrorAsync(toString, b).WaitAndUnwrapException();
    }

    public IList<IListBlockContext<TItem, THeader>> GetListBlocks<TItem, THeader>(
        Func<IFluentListBlockDescriptorBase<TItem, THeader>, object> fluentBlockRequest)
    {
        return GetListBlocksAsync(fluentBlockRequest).WaitAndUnwrapException();
    }

    public IListBlock<TItem, THeader> GetLastListBlock<TItem, THeader>()
    {
        return GetLastListBlockAsync<TItem, THeader>().WaitAndUnwrapException();
    }

    ~TaskExecutionContext()
    {
        Dispose(false);
    }

    protected virtual void Dispose(bool disposing)
    {
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
        return GetDateRangeBlocksAsync(fluentBlockRequest).WaitAndUnwrapException();
    }

    public async Task<IList<T>> GetBlocksAsync<T, U, TBlockRequest, TBlockSettings>(BlockType blockType, Func<U, object> fluentBlockRequest,
        Func<TBlockSettings, TBlockRequest> createRequestFunc,
        Func<TBlockRequest, Task<IList<T>>> generateFunc)
        where TBlockRequest : BlockRequest where U : new() where TBlockSettings : IBlockSettings
    {
        _logger.LogDebug($"Entering block request for {typeof(T).Name}");
        try
        {
            if (!IsExecutionContextActive) throw new ExecutionException(NotActiveMessage);

            var fluentDescriptor = fluentBlockRequest(new U());
            var settings = (TBlockSettings)fluentDescriptor;
            if (settings.BlockType == blockType)
            {
                var request = createRequestFunc(settings);
                if (ShouldProtect(request))
                {
                    _logger.LogDebug("Request is protected - creating critical section");
                    var csContext = CreateClientCriticalSection();
                    try
                    {
                        var csStarted = await csContext.TryStartAsync(_startupOptions.CriticalSectionRetry,
                            _startupOptions.CriticalSectionAttemptCount).ConfigureAwait(false);
                        if (csStarted) return await generateFunc(request).ConfigureAwait(false);

                        throw new CriticalSectionException("Could not start a critical section in the alloted time");
                    }
                    finally
                    {
                        await csContext.CompleteAsync().ConfigureAwait(false);
                    }
                }

                _logger.LogDebug("Request is unprotected");
                return await generateFunc(request).ConfigureAwait(false);
            }

            throw new NotSupportedException($"BlockType {blockType} not supported");
        }
        finally
        {
            _logger.LogDebug($"Exiting block request for {typeof(T).Name}");
        }
    }

    private void CleanUpOldData()
    {
        _cleanUpService.CleanOldData(_taskExecutionInstance.TaskId,
            _taskExecutionInstance.TaskExecutionId, _taskConfigurationRepository);
    }

    private TaskExecutionStartRequest CreateStartRequest(Guid referenceValue)
    {
        var startRequest = new TaskExecutionStartRequest(
            _taskExecutionInstance.TaskId,
            _taskExecutionOptions.TaskDeathMode,
            _taskExecutionOptions.ConcurrencyLimit,
            _taskConfiguration.FailedTaskRetryLimit,
            _taskConfiguration.DeadTaskRetryLimit);
        _logger.LogDebug($"{nameof(startRequest)}={Constants.Serialize(startRequest)}");

        SetStartRequestValues(startRequest, referenceValue);
        SetStartRequestTasklingVersion(startRequest);
        SerializeHeaderIfExists(startRequest);

        return startRequest;
    }

    private void SetStartRequestTasklingVersion(TaskExecutionStartRequest startRequest)
    {
        var assembly = Assembly.GetExecutingAssembly();
        var fileVersionInfo = FileVersionInfo.GetVersionInfo(assembly.Location);
        var version = fileVersionInfo.ProductVersion;
        startRequest.TasklingVersion = version;
    }

    private void SetStartRequestValues(TaskExecutionStartRequest startRequest, Guid referenceValue)
    {
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
        if (_taskExecutionHeader != null)
        {
            startRequest.TaskExecutionHeader = JsonGenericSerializer.Serialize(_taskExecutionHeader);
        }
    }

    private void StartKeepAlive()
    {
        var keepAliveRequest = new SendKeepAliveRequest(_taskExecutionInstance.TaskId)
        {
            TaskExecutionId = _taskExecutionInstance.TaskExecutionId,
            ExecutionTokenId = _taskExecutionInstance.ExecutionTokenId
        };

        _keepAliveDaemon = new KeepAliveDaemon(_taskExecutionRepository, new WeakReference(this),
            _loggerFactory.CreateLogger<KeepAliveDaemon>());
        _keepAliveDaemon.Run(keepAliveRequest, _taskExecutionOptions.KeepAliveInterval.Value);
    }

    private DateRangeBlockRequest GetDateRangeBlockRequest(IBlockSettings settings)
    {
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

    private NumericRangeBlockRequest GetNumericRangeBlockRequest(IBlockSettings settings)
    {
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
        var tmp = _userCriticalSectionContext != null && _userCriticalSectionContext.IsActive();
        _logger.LogDebug($"{nameof(IsUserCriticalSectionActive)} = {tmp}");
        return tmp;
    }

    private bool ShouldProtect(BlockRequest blockRequest)
    {
        var tmp = (blockRequest.ReprocessDeadTasks || blockRequest.ReprocessFailedTasks) &&
                  !IsUserCriticalSectionActive();
        _logger.LogDebug(tmp.ToString());
        return tmp;
    }

    private ICriticalSectionContext CreateClientCriticalSection()
    {
        if (IsClientCriticalSectionActive())
            throw new CriticalSectionException("Only one client critical section context can be active at a time");

        _clientCriticalSectionContext = new CriticalSectionContext(_criticalSectionRepository,
            _taskExecutionInstance,
            _taskExecutionOptions,
            CriticalSectionType.Client, _startupOptions, _loggerFactory.CreateLogger<CriticalSectionContext>());

        return _clientCriticalSectionContext;
    }

    private bool IsClientCriticalSectionActive()
    {
        return _clientCriticalSectionContext != null && _clientCriticalSectionContext.IsActive();
    }

    private TaskExecutionMetaRequest CreateTaskExecutionMetaRequest(int numberToRetrieve)
    {
        var request = new TaskExecutionMetaRequest(_taskExecutionInstance.TaskId);

        request.ExecutionsToRetrieve = numberToRetrieve;

        return request;
    }
}