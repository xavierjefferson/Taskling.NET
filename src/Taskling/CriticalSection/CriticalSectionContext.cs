﻿using System;
using System.Threading.Tasks;
using Taskling.Contexts;
using Taskling.Exceptions;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Tasks;

namespace Taskling.CriticalSection;

public class CriticalSectionContext : ICriticalSectionContext
{
    private readonly ICriticalSectionRepository _criticalSectionRepository;
    private readonly CriticalSectionType _criticalSectionType;
    private readonly TaskExecutionInstance _taskExecutionInstance;
    private readonly TaskExecutionOptions _taskExecutionOptions;
    private bool _completeCalled;

    private bool _started;

    private bool disposed;

    public CriticalSectionContext(ICriticalSectionRepository criticalSectionRepository,
        TaskExecutionInstance taskExecutionInstance,
        TaskExecutionOptions taskExecutionOptions,
        CriticalSectionType criticalSectionType)
    {
        _criticalSectionRepository = criticalSectionRepository;
        _taskExecutionInstance = taskExecutionInstance;
        _taskExecutionOptions = taskExecutionOptions;
        _criticalSectionType = criticalSectionType;

        ValidateOptions();
    }

    public bool IsActive()
    {
        return _started && !_completeCalled;
    }

    public async Task<bool> TryStartAsync()
    {
        return await TryStartAsync(new TimeSpan(0, 0, 30), 3).ConfigureAwait(false);
    }

    public async Task<bool> TryStartAsync(TimeSpan retryInterval, int numberOfAttempts)
    {
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
        if (!_started || _completeCalled)
            throw new ExecutionException("There is no active critical section to complete");

        var completeRequest = new CompleteCriticalSectionRequest(
            new TaskId(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName),
            _taskExecutionInstance.TaskExecutionId,
            _criticalSectionType);

        await _criticalSectionRepository.CompleteAsync(completeRequest).ConfigureAwait(false);

        _completeCalled = true;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    ~CriticalSectionContext()
    {
        Dispose(false);
    }

    protected virtual void Dispose(bool disposing)
    {
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
        if (_started)
            throw new ExecutionException("There is already an active critical section");

        _started = true;

        var startRequest = new StartCriticalSectionRequest(
            new TaskId(_taskExecutionInstance.ApplicationName, _taskExecutionInstance.TaskName),
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