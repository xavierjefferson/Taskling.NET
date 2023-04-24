using System;
using System.Threading.Tasks;
using Taskling.Exceptions;

namespace Taskling.Retries;

public class RetryService
{
    public static async Task InvokeWithRetryAsync<RQ>(Func<RQ, Task> requestAction, RQ request)
    {
        var interval = 5000;
        double publishExponentialBackoffExponent = 2;
        var attemptLimit = 3;
        var attemptsMade = 0;

        var successFullySent = false;
        Exception lastException = null;

        while (attemptsMade < attemptLimit && successFullySent == false)
        {
            try
            {
                await requestAction(request).ConfigureAwait(false);
                successFullySent = true;
            }
            catch (TransientException ex)
            {
                lastException = ex;
            }

            interval = (int)(interval * publishExponentialBackoffExponent);
            attemptsMade++;

            if (!successFullySent)
                await Task.Delay(interval).ConfigureAwait(false);
        }

        if (!successFullySent)
            throw new ExecutionException("A persistent transient exception has caused all retries to fail",
                lastException);
    }
}