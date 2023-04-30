using System.Data.SqlClient;
using System.Transactions;
using Taskling.SqlServer.AncilliaryServices;

namespace Taskling.SqlServer.Blocks;

public class RetryHelper
{
    private const int DefaultMaxDelayMilliseconds = 2000;
    private const int DefaultMaxRetries = 10;
    private const int DefaultDelayMilliseconds = 1000;

    public static async Task WithRetry(Func<Task> action,
        int maxRetries = DefaultMaxRetries, int maxDelayMilliseconds = DefaultMaxDelayMilliseconds,
        int delayMilliseconds = DefaultDelayMilliseconds)
    {
        var retryInfo = new RetryInfo(maxRetries, maxDelayMilliseconds, delayMilliseconds);

        while (retryInfo.Exceptions.Count < maxRetries)
            //for (var retry = 0; retry < maxRetries; retry++)
            try
            {
                using (var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                {
                    await action().ConfigureAwait(false);
                    transactionScope.Complete();
                    return;
                }
            }
            catch (Exception ex)
            {
                await retryInfo.ProcessExceptionAsync(ex);
            }

        throw new AggregateException(retryInfo.Exceptions);
    }

    public static void WithRetry(Action action,
        int maxRetries = DefaultMaxRetries, int maxDelayMilliseconds = DefaultMaxDelayMilliseconds,
        int delayMilliseconds = DefaultDelayMilliseconds)
    {
        var retryInfo = new RetryInfo(maxRetries, maxDelayMilliseconds, delayMilliseconds);

        while (retryInfo.Exceptions.Count < maxRetries)
            //for (var retry = 0; retry < maxRetries; retry++)
            try
            {
                using (var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                {
                    action();
                    transactionScope.Complete();
                    return;
                }
            }
            catch (Exception ex)
            {
                retryInfo.ProcessException(ex);
            }

        throw new AggregateException(retryInfo.Exceptions);
    }

    public static T WithRetry<T>(Func<T> action,
        int maxRetries = DefaultMaxRetries, int maxDelayMilliseconds = DefaultMaxDelayMilliseconds,
        int delayMilliseconds = DefaultDelayMilliseconds)
    {
        var retryInfo = new RetryInfo(maxRetries, maxDelayMilliseconds, delayMilliseconds);


        while (retryInfo.Exceptions.Count < maxRetries)
            //for (var retry = 0; retry < maxRetries; retry++)
            try
            {
                using (var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                {
                    var tmp = action();
                    transactionScope.Complete();
                    return tmp;
                }
            }
            catch (Exception ex)
            {
                retryInfo.ProcessException(ex);
            }

        throw new AggregateException(retryInfo.Exceptions);
    }

    private static SqlExceptionInfo? FindSqlExceptionInfo(Exception ex)
    {
        if (ex is SqlException sq) return new SqlExceptionInfo { Number = sq.Number };

        if (ex is Microsoft.Data.SqlClient.SqlException sk) return new SqlExceptionInfo { Number = sk.Number };


        if (ex.InnerException != null) return FindSqlExceptionInfo(ex.InnerException);

        return null;
    }

    public static async Task<T> WithRetry<T>(Func<Task<T>> action,
        int maxRetries = DefaultMaxRetries, int maxDelayMilliseconds = DefaultMaxDelayMilliseconds,
        int delayMilliseconds = DefaultDelayMilliseconds)
    {
        var r0 = new RetryInfo(maxRetries, maxDelayMilliseconds, delayMilliseconds);

        while (r0.Exceptions.Count < maxRetries)
            //for (var retry = 0; retry < maxRetries; retry++)
            try
            {
                using (var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                {

                    var tmp = await action().ConfigureAwait(false);
                    transactionScope.Complete();
                    return tmp;
                }
            }
            catch (Exception ex)
            {
                await r0.ProcessExceptionAsync(ex).ConfigureAwait(false);
            }

        throw new AggregateException(r0.Exceptions);
    }

    public class SqlExceptionInfo
    {
        public int Number { get; set; }
    }

    private class RetryInfo
    {
        public RetryInfo(int maxRetries = DefaultMaxRetries, int maxDelayMilliseconds = DefaultMaxDelayMilliseconds,
            int delayMilliseconds = DefaultDelayMilliseconds)
        {
            Backoff = new ExponentialBackoff(delayMilliseconds, maxDelayMilliseconds);

            Exceptions = new List<Exception>();
        }

        public List<Exception> Exceptions { get; }
        public ExponentialBackoff Backoff { get; }

        public async Task ProcessExceptionAsync(Exception ex)
        {
            var sqlExceptionInfo = FindSqlExceptionInfo(ex);
            if (sqlExceptionInfo == null)
                Exceptions.Add(ex);
            else if (!TransientErrorDetector.IsTransient(sqlExceptionInfo))
                Exceptions.Add(ex);

            await Backoff.DelayAsync()
                .ConfigureAwait(false);
        }

        public async void ProcessException(Exception ex)
        {
            var sqlExceptionInfo = FindSqlExceptionInfo(ex);
            if (sqlExceptionInfo == null)
                Exceptions.Add(ex);
            else if (!TransientErrorDetector.IsTransient(sqlExceptionInfo))
                Exceptions.Add(ex);

            await Backoff.DelayAsync()
                .ConfigureAwait(false);
        }
    }

    public struct ExponentialBackoff
    {
        private readonly int _delayMilliseconds;
        private readonly int _maxDelayMilliseconds;
        private int _retries;
        private int _pow;

        public ExponentialBackoff(int delayMilliseconds, int maxDelayMilliseconds)
        {
            _delayMilliseconds = delayMilliseconds;
            _maxDelayMilliseconds = maxDelayMilliseconds;
            _retries = 0;
            _pow = 1;
        }

        public void Delay()
        {
            ++_retries;
            if (_retries < 31) _pow = _pow << 1; // m_pow = Pow(2, m_retries - 1)

            var delay = Math.Min(_delayMilliseconds * (_pow - 1) / 2, _maxDelayMilliseconds);
            Thread.Sleep(delay);
        }

        public Task DelayAsync()
        {
            ++_retries;
            if (_retries < 31) _pow = _pow << 1; // m_pow = Pow(2, m_retries - 1)

            var delay = Math.Min(_delayMilliseconds * (_pow - 1) / 2, _maxDelayMilliseconds);
            return Task.Delay(delay);
        }
    }
}