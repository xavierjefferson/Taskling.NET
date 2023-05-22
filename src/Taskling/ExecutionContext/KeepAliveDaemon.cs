using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.ExecutionContext;

internal class KeepAliveDaemon
{
    private readonly ILogger<KeepAliveDaemon> _logger;
    private readonly WeakReference _owner;
    private readonly ITaskExecutionRepository _taskExecutionRepository;
    private bool _completeCalled;

    public KeepAliveDaemon(ITaskExecutionRepository taskExecutionRepository, WeakReference owner,
        ILogger<KeepAliveDaemon> logger)
    {
        _logger = logger;
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