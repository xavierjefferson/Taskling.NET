using System;
using System.Threading.Tasks;
using Polly;
using Taskling.Exceptions;

namespace Taskling.Retries;

public class RetryService
{
    public static async Task InvokeWithRetryAsync<RQ>(Func<RQ, Task> requestAction, RQ request)
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