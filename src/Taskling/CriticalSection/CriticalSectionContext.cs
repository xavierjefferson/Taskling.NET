using System;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx.Synchronous;
using Taskling.Contexts;
using Taskling.Exceptions;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Tasks;

namespace Taskling.CriticalSection;

public class CriticalSectionContext : ICriticalSectionContext
{
    private readonly ICriticalSectionRepository _criticalSectionRepository;
    private readonly CriticalSectionType _criticalSectionType;
    private readonly ILogger<CriticalSectionContext> _logger;
    private readonly TaskExecutionInstance _taskExecutionInstance;
    private readonly TaskExecutionOptions _taskExecutionOptions;
    private readonly TasklingOptions _tasklingOptions;
    private bool _completeCalled;

    private bool _started;

    private bool disposed;

    public CriticalSectionContext(ICriticalSectionRepository criticalSectionRepository,
        TaskExecutionInstance taskExecutionInstance,
        TaskExecutionOptions taskExecutionOptions,
        CriticalSectionType criticalSectionType, TasklingOptions tasklingOptions,
        ILogger<CriticalSectionContext> logger)
    {
        _logger = logger;
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _criticalSectionRepository = criticalSectionRepository;
        _taskExecutionInstance = taskExecutionInstance;
        _taskExecutionOptions = taskExecutionOptions;
        _criticalSectionType = criticalSectionType;
        _tasklingOptions = tasklingOptions;


        ValidateOptions();
    }

    public bool IsActive()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return _started && !_completeCalled;
    }

    public async Task<bool> TryStartAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return await TryStartAsync(_tasklingOptions.CriticalSectionRetry, _tasklingOptions.CriticalSectionAttemptCount)
            .ConfigureAwait(false);
    }

    public async Task<bool> TryStartAsync(TimeSpan retryInterval, int numberOfAttempts)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var tryCount = 0;
        var started = false;

        while (started == false && tryCount <= numberOfAttempts)
        {
            tryCount++;
            started = await TryStartCriticalSectionAsync().ConfigureAwait(false);
            if (!started)
                await Task.Delay(retryInterval).ConfigureAwait(false);
        }

        return started;
    }

    public async Task CompleteAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (!_started || _completeCalled)
            throw new ExecutionException("There is no active critical section to complete");

        var completeRequest = new CompleteCriticalSectionRequest(
            _taskExecutionInstance.TaskId,
            _taskExecutionInstance.TaskExecutionId,
            _criticalSectionType);

        await _criticalSectionRepository.CompleteAsync(completeRequest).ConfigureAwait(false);

        _completeCalled = true;
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

    ~CriticalSectionContext()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        Dispose(false);
    }

    protected virtual void Dispose(bool disposing)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (disposed)
            return;

        if (disposing)
        {
        }

        if (_started && !_completeCalled)
            Task.Run(async () => await CompleteAsync().ConfigureAwait(false));

        disposed = true;
    }

    private async Task<bool> TryStartCriticalSectionAsync()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (_started)
            throw new ExecutionException("There is already an active critical section");

        _started = true;

        var startRequest = new StartCriticalSectionRequest(
            _taskExecutionInstance.TaskId,
            _taskExecutionInstance.TaskExecutionId,
            _taskExecutionOptions.TaskDeathMode,
            _criticalSectionType);

        if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.Override)
            startRequest.OverrideThreshold = _taskExecutionOptions.OverrideThreshold.Value;

        if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
            startRequest.KeepAliveDeathThreshold = _taskExecutionOptions.KeepAliveDeathThreshold.Value;

        var response = await _criticalSectionRepository.StartAsync(startRequest).ConfigureAwait(false);
        if (response.GrantStatus == GrantStatus.Denied)
        {
            _started = false;
            return false;
        }

        return true;
    }

    private void ValidateOptions()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.LogDebug($"TaskDeathMode={_taskExecutionOptions.TaskDeathMode}");
        if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.KeepAlive)
        {
            if (!_taskExecutionOptions.KeepAliveDeathThreshold.HasValue)
                throw new ExecutionArgumentsException("KeepAliveElapsed must be set when using KeepAlive mode");

            if (!_taskExecutionOptions.KeepAliveInterval.HasValue)
                throw new ExecutionArgumentsException("KeepAliveInterval must be set when using KeepAlive mode");
        }
        else if (_taskExecutionOptions.TaskDeathMode == TaskDeathMode.Override)
        {
            if (!_taskExecutionOptions.OverrideThreshold.HasValue)
                throw new ExecutionArgumentsException("SecondsOverride must be set when using Override mode");
        }
    }
}