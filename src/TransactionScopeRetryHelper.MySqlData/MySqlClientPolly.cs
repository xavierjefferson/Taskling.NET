using MySql.Data.MySqlClient;
using Polly;

namespace TransactionScopeRetryHelper.MySqlClient;

public class MySqlClientPolly : IPollyCheck
{
    public PolicyBuilder<T> Add<T>(PolicyBuilder<T> input)
    {
        return input.Or<MySqlException>(IsTransient).OrInner<MySqlException>(IsTransient);
    }

    public PolicyBuilder Add(PolicyBuilder input)
    {
        return input.Or<MySqlException>(IsTransient).OrInner<MySqlException>(IsTransient);
    }

    private bool IsTransient(MySqlException exception)
    {
        if (exception.IsTransient) return true;
        switch (exception.Number)
        {
            case 40001:
                return true;
            default:
                return false;
        }
    }
}