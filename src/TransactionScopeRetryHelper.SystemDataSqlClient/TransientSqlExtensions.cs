namespace TransactionScopeRetryHelper.SystemDataSqlClient;

public static class TransientSqlExtensions
{
    public static PollyExtension AddSystemDataSqlClient(this PollyExtension list)
    {
        RetryHelper.Extensions.Add(new SystemDataSqlClientPolly());
        return RetryHelper.Extensions;
    }
}