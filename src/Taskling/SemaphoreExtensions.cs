using System;
using System.Threading;
using System.Threading.Tasks;

namespace Taskling;

public static class SemaphoreExtensions
{
    public static void Wrap(this SemaphoreSlim semaphoreSlim, Action action)
    {
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