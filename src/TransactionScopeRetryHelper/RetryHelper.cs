using System.Transactions;
using Polly;

namespace TransactionScopeRetryHelper;

public class RetryHelper
{
    private const int DefaultMaxDelayMilliseconds = 2000;
    private const int DefaultMaxRetries = 10;
    private const int DefaultDelayMilliseconds = 1000;

    public static ExceptionChecklist TransientCheckFunctions { get; } = new();
    public static PollyExtension Extensions { get; } = new();

    public static async Task WithRetryAsync(Func<Task> action,
        int maxRetries = DefaultMaxRetries, int maxDelayMilliseconds = DefaultMaxDelayMilliseconds,
        int delayMilliseconds = DefaultDelayMilliseconds)
    {
        var sleepDurationProvider = new SleepDurationProvider(maxDelayMilliseconds, delayMilliseconds);
        var myPolicy = GetPolicy().WaitAndRetryAsync(maxRetries, sleepDurationProvider.GetDelay);

        await myPolicy.ExecuteAsync(async () =>
        {
            using (var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                await action().ConfigureAwait(false);
                transactionScope.Complete();
            }
        }).ConfigureAwait(false);
    }

    public static void WithRetry(Action action,
        int maxRetries = DefaultMaxRetries, int maxDelayMilliseconds = DefaultMaxDelayMilliseconds,
        int delayMilliseconds = DefaultDelayMilliseconds)
    {
        var sleepDurationProvider = new SleepDurationProvider(maxDelayMilliseconds, delayMilliseconds);
        var myPolicy = GetPolicy().WaitAndRetry(maxRetries, sleepDurationProvider.GetDelay);
        myPolicy.Execute(() =>
        {
            using (var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                action();
                transactionScope.Complete();
            }
        });
    }

    public static T WithRetry<T>(Func<T> action,
        int maxRetries = DefaultMaxRetries, int maxDelayMilliseconds = DefaultMaxDelayMilliseconds,
        int delayMilliseconds = DefaultDelayMilliseconds)
    {
        var sleepDurationProvider = new SleepDurationProvider(maxDelayMilliseconds, delayMilliseconds);
        var myPolicy = GetGenericPolicy<T>().WaitAndRetry(maxRetries, sleepDurationProvider.GetDelay);

        return myPolicy.Execute(() =>
        {
            using (var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                var tmp = action();
                transactionScope.Complete();
                return tmp;
            }
        });
    }

    private static PolicyBuilder GetPolicy()
    {
        var policyBuilder = Policy.Handle<Exception>(i => false);

        foreach (var extension in Extensions) policyBuilder = extension.Add(policyBuilder);

        return policyBuilder;
    }

    private static PolicyBuilder<T> GetGenericPolicy<T>()
    {
        var policyBuilder = Policy<T>.Handle<Exception>(i => false);

        foreach (var extension in Extensions)
        {
            policyBuilder = extension.Add(policyBuilder);
        }

        return policyBuilder;
    }


    public static async Task<T> WithRetryAsync<T>(Func<Task<T>> action,
        int maxRetries = DefaultMaxRetries, int maxDelayMilliseconds = DefaultMaxDelayMilliseconds,
        int delayMilliseconds = DefaultDelayMilliseconds)
    {
        var sleepDurationProvider = new SleepDurationProvider(maxDelayMilliseconds, delayMilliseconds);
        var myPolicy = GetGenericPolicy<T>().WaitAndRetryAsync(maxRetries, sleepDurationProvider.GetDelay);


        return await myPolicy.ExecuteAsync(async () =>
        {
            using (var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                var tmp = await action().ConfigureAwait(false);
                transactionScope.Complete();
                return tmp;
            }
        }).ConfigureAwait(false);
    }


    private class SleepDurationProvider
    {
        private readonly int _delayMilliseconds;
        private readonly int _maxDelayMilliseconds;
        private int _pow;

        public SleepDurationProvider(int maxDelayMilliseconds = DefaultMaxDelayMilliseconds,
            int delayMilliseconds = DefaultDelayMilliseconds)
        {
            _delayMilliseconds = delayMilliseconds;
            _maxDelayMilliseconds = maxDelayMilliseconds;
            _pow = 1;
        }

        public TimeSpan GetDelay(int attempt)
        {
            if (attempt < 31) _pow = _pow << 1; // m_pow = Pow(2, m_retries - 1)

            var delay = Math.Min(_delayMilliseconds * (_pow - 1) / 2, _maxDelayMilliseconds);
            return TimeSpan.FromMilliseconds(delay);
        }
    }

    //private class RetryInfo
    //{
    //    public RetryInfo(int maxRetries = DefaultMaxRetries, int maxDelayMilliseconds = DefaultMaxDelayMilliseconds,
    //        int delayMilliseconds = DefaultDelayMilliseconds)
    //    {
    //        Backoff = new ExponentialBackoff(delayMilliseconds, maxDelayMilliseconds);

    //        Exceptions = new List<Exception>();
    //    }

    //    public List<Exception> Exceptions { get; }
    //    public ExponentialBackoff Backoff { get; }

    //    public async Task ProcessExceptionAsync(Exception ex)
    //    {
    //        var isTransient = TransientCheckFunctions.Any(i => i(ex));
    //        if (!isTransient) Exceptions.Add(ex);
    //        await Backoff.DelayAsync()
    //            .ConfigureAwait(false);
    //    }

    //    public void ProcessException(Exception ex)
    //    {
    //        var isTransient = TransientCheckFunctions.Any(i => i(ex));
    //        if (!isTransient) Exceptions.Add(ex);


    //        Backoff.Delay();
    //    }
    //}

    //public struct ExponentialBackoff
    //{
    //    private readonly int _delayMilliseconds;
    //    private readonly int _maxDelayMilliseconds;
    //    private int _retries;
    //    private int _pow;

    //    public ExponentialBackoff(int delayMilliseconds, int maxDelayMilliseconds)
    //    {
    //        _delayMilliseconds = delayMilliseconds;
    //        _maxDelayMilliseconds = maxDelayMilliseconds;
    //        _retries = 0;
    //        _pow = 1;
    //    }

    //    public void Delay()
    //    {
    //        ++_retries;
    //        if (_retries < 31) _pow = _pow << 1; // m_pow = Pow(2, m_retries - 1)

    //        var delay = Math.Min(_delayMilliseconds * (_pow - 1) / 2, _maxDelayMilliseconds);
    //        Thread.Sleep(delay);
    //    }

    //    public Task DelayAsync()
    //    {
    //        ++_retries;
    //        if (_retries < 31) _pow = _pow << 1; // m_pow = Pow(2, m_retries - 1)

    //        var delay = Math.Min(_delayMilliseconds * (_pow - 1) / 2, _maxDelayMilliseconds);
    //        return Task.Delay(delay);
    //    }
    //}
}