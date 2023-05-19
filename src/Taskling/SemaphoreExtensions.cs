using System;
using System.Threading;
using System.Threading.Tasks;

namespace Taskling;

public static class SemaphoreExtensions
{
    public static void Wrap(this SemaphoreSlim semaphoreSlim, Action action)
    {
        //_logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        try
        {
            semaphoreSlim.Wait();
            action();
        }
        finally
        {
            semaphoreSlim.Release();
        }
    }

    public static T Wrap<T>(this SemaphoreSlim semaphoreSlim, Func<T> func)
    {
        // _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        try
        {
            semaphoreSlim.Wait();
            return func();
        }
        finally
        {
            semaphoreSlim.Release();
        }
    }

    public static async Task WrapAsync(this SemaphoreSlim semaphoreSlim, Func<Task> func)
    {
        //_logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        try
        {
            await semaphoreSlim.WaitAsync().ConfigureAwait(false);
            await func().ConfigureAwait(false);
        }
        finally
        {
            semaphoreSlim.Release();
        }
    }

    public static async Task<T> WrapAsync<T>(this SemaphoreSlim semaphoreSlim, Func<Task<T>> func)
    {
        //_logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        try
        {
            await semaphoreSlim.WaitAsync().ConfigureAwait(false);
            return await func();
        }
        finally
        {
            semaphoreSlim.Release();
        }
    }
}