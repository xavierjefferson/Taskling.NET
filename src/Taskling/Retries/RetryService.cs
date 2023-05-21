using System;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Polly;
using Taskling.Extensions;

namespace Taskling.Retries;

public interface IRetryService
{
    Task InvokeWithRetryAsync<RQ>(Func<RQ, Task> requestAction, RQ request);
}

public class RetryService : IRetryService
{
    private readonly ILogger<RetryService> _logger;

    public RetryService(ILogger<RetryService> logger)
    {
        _logger = logger;
    }

    public async Task InvokeWithRetryAsync<RQ>(Func<RQ, Task> requestAction, RQ request)
    {
        const double publishExponentialBackoffExponent = 2;
        const int attemptLimit = 3;
        const int interval = 5000;

        TimeSpan SleepDurationProvider(int attempt)
        {
            var value = Math.Pow(publishExponentialBackoffExponent, attempt - 1) * interval;
            return TimeSpan.FromMilliseconds(value);
        }

        var asyncRetryPolicy = Policy.Handle<Exception>().WaitAndRetryAsync(attemptLimit, SleepDurationProvider);
        await asyncRetryPolicy.ExecuteAsync(() => requestAction(request));
    }
}