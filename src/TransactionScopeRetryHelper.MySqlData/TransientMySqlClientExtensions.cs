using MySql.Data.MySqlClient;
using Polly;

namespace TransactionScopeRetryHelper.MySqlClient;

public class MySqlClientPolly : IPollyCheck
{
    bool IsTransient(MySqlException exception)
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
    public PolicyBuilder<T> Add<T>(PolicyBuilder<T> input)
    {
        return input.Or<MySqlException>(IsTransient).OrInner<MySqlException>(IsTransient);
    }

    public PolicyBuilder Add(PolicyBuilder input)
    {
        return input.Or<MySqlException>(IsTransient).OrInner<MySqlException>(IsTransient);
    }
}
public static class TransientMySqlClientExtensions
{
    private static bool IsTransient(int? number)
    {
        if (number == 40001 // 1205 = Deadlock
            //|| sqlEx.Number == -2 // -2 = TimeOut
            //|| sqlEx.Number == -1 // -1 = Connection
            //|| sqlEx.Number == 2 // 2 = Connection
            //|| sqlEx.Number == 53 // 53 = Connection
           )
            return true;

        return false;
    }


    private static int? FindSqlExceptionInfo(Exception ex)
    {
        if (ex is MySqlException mySqlException) return mySqlException.Number;


        if (ex.InnerException != null) return FindSqlExceptionInfo(ex.InnerException);

        return null;
    }
    public static PollyExtension AddMySqlClient(this PollyExtension list)
    {
        RetryHelper.Extensions.Add(new MySqlClientPolly());
        return RetryHelper.Extensions;
    }
    public static ExceptionChecklist AddMySqlClient(this ExceptionChecklist list)
    {
        RetryHelper.TransientCheckFunctions.Add(i =>
        {
            var number = FindSqlExceptionInfo(i);
            return number != null && IsTransient(number);
        });
        return RetryHelper.TransientCheckFunctions;
    }
}