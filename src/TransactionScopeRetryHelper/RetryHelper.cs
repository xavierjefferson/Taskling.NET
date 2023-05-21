using System.Data.Common;
using System.Transactions;
using Polly;

namespace TransactionScopeRetryHelper;

public class RetryHelper
{
    private const int DefaultMaxDelayMilliseconds = 10000;
    private const int DefaultMaxRetries = 10;
    private const int DefaultDelayMilliseconds = 5000;

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
        var policyBuilder = Policy.Handle<DbException>(i => i.IsTransient).OrInner<DbException>(i => i.IsTransient);

        foreach (var extension in Extensions) policyBuilder = extension.Add(policyBuilder);

        return policyBuilder;
    }

    private static PolicyBuilder<T> GetGenericPolicy<T>()
    {
        var policyBuilder = Policy<T>.Handle<DbException>(i => i.IsTransient).OrInner<DbException>(i => i.IsTransient);

        foreach (var extension in Extensions) policyBuilder = extension.Add(policyBuilder);

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
}