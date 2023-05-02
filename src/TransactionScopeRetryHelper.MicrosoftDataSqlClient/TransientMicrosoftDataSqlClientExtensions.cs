namespace TransactionScopeRetryHelper.MicrosoftDataSqlClient;

public static class TransientMicrosoftDataSqlClientExtensions
{
    public static PollyExtension AddMicrosoftDataSqlClient(this PollyExtension list)
    {
        RetryHelper.Extensions.Add(new MicrosoftDataSqlClientPolly());
        return RetryHelper.Extensions;
    }
}