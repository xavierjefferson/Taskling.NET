using Microsoft.Data.SqlClient;
using Polly;

namespace TransactionScopeRetryHelper.MicrosoftDataSqlClient;

public class MicrosoftDataSqlClientPolly : IPollyCheck
{
    public PolicyBuilder<T> Add<T>(PolicyBuilder<T> input)
    {
        return input.Or<SqlException>(IsTransient).OrInner<SqlException>(IsTransient);
    }

    public PolicyBuilder Add(PolicyBuilder input)
    {
        return input.Or<SqlException>(IsTransient).OrInner<SqlException>(IsTransient);
    }

    private bool IsTransient(SqlException exception)
    {
        if (exception.IsTransient) return true;
        switch (exception.Number)
        {
            case 1205:
            case -2:
            case -1:
            case 2:
            // 53 = Connection
            case 53:
                return true;
            default:
                return false;
        }
    }
}