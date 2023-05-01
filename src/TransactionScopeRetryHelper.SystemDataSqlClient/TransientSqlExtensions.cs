using Polly;
using System.Data.SqlClient;

namespace TransactionScopeRetryHelper.SystemDataSqlClient;
public class SystemDataSqlClientPolly : IPollyCheck
{
    bool IsTransient(SqlException exception)
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
    public PolicyBuilder<T> Add<T>(PolicyBuilder<T> input)
    {
        return input.Or<SqlException>(IsTransient).OrInner<SqlException>(IsTransient);
    }

    public PolicyBuilder Add(PolicyBuilder input)
    {
        return input.Or<SqlException>(IsTransient).OrInner<SqlException>(IsTransient);
    }
}
public static class TransientSqlExtensions
{
    private static bool IsTransient(SqlExceptionInfo sqlEx)
    {
        if (sqlEx.Number == 1205 // 1205 = Deadlock
            || sqlEx.Number == -2 // -2 = TimeOut
            || sqlEx.Number == -1 // -1 = Connection
            || sqlEx.Number == 2 // 2 = Connection
            || sqlEx.Number == 53 // 53 = Connection
           )
            return true;

        return false;
    }

    private static bool IsTransient(SqlException sqlEx)
    {
        if (sqlEx.Number == 1205 // 1205 = Deadlock
            || sqlEx.Number == -2 // -2 = TimeOut
            || sqlEx.Number == -1 // -1 = Connection
            || sqlEx.Number == 2 // 2 = Connection
            || sqlEx.Number == 53 // 53 = Connection
           )
            return true;

        return false;
    }

    private static SqlExceptionInfo? FindSqlExceptionInfo(Exception ex)
    {
        if (ex is SqlException sq) return new SqlExceptionInfo { Number = sq.Number };


        if (ex.InnerException != null) return FindSqlExceptionInfo(ex.InnerException);

        return null;
    }
    public static PollyExtension AddSystemDataSqlClient(this PollyExtension list)
    {
        RetryHelper.Extensions.Add(new SystemDataSqlClientPolly());
        return RetryHelper.Extensions;
    }
    public static ExceptionChecklist AddSystemDataSqlClient(this ExceptionChecklist list)
    {
        list.Add(i =>
        {
            var sqlExceptionInfo = FindSqlExceptionInfo(i);
            return sqlExceptionInfo != null && IsTransient(sqlExceptionInfo);
        });
        return list;
    }

    public class SqlExceptionInfo
    {
        public int Number { get; set; }
    }
}