﻿using System;
using System.Threading.Tasks;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.ExecutionContext;

internal class KeepAliveDaemon
{
    private readonly ITaskExecutionRepository _taskExecutionRepository;
    private bool _completeCalled;
    private readonly WeakReference _owner;

    public KeepAliveDaemon(ITaskExecutionRepository taskExecutionRepository, WeakReference owner)
    {
        _owner = owner;
        _taskExecutionRepository = taskExecutionRepository;
    }

    public void Stop()
    {
        _completeCalled = true;
    }

    public void Run(SendKeepAliveRequest sendKeepAliveRequest, TimeSpan keepAliveInterval)
    {
        Task.Run(async () => await StartKeepAliveAsync(sendKeepAliveRequest, keepAliveInterval).ConfigureAwait(false));
    }

    private async Task StartKeepAliveAsync(SendKeepAliveRequest sendKeepAliveRequest, TimeSpan keepAliveInterval)
    {
        var lastKeepAlive = DateTime.UtcNow;
        await _taskExecutionRepository.SendKeepAliveAsync(sendKeepAliveRequest).ConfigureAwait(false);

        while (!_completeCalled && _owner.IsAlive)
        {
            var timespanSinceLastKeepAlive = DateTime.UtcNow - lastKeepAlive;
            if (timespanSinceLastKeepAlive > keepAliveInterval)
            {
                lastKeepAlive = DateTime.UtcNow;
                await _taskExecutionRepository.SendKeepAliveAsync(sendKeepAliveRequest).ConfigureAwait(false);
            }

            await Task.Delay(1000).ConfigureAwait(false);
        }
    }
}