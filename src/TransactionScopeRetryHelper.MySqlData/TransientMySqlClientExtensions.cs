namespace TransactionScopeRetryHelper.MySqlClient;

public static class TransientMySqlClientExtensions
{
    public static PollyExtension AddMySqlClient(this PollyExtension list)
    {
        RetryHelper.Extensions.Add(new MySqlClientPolly());
        return RetryHelper.Extensions;
    }
}