using System.Data;
using System.Data.Common;

namespace Taskling.SqlServer;

internal static class DbDataReaderExtensions
{
    public static TimeSpan GetTimeSpan(this DbDataReader reader, string columnName)
    {
        return (TimeSpan)reader[columnName];
    }

    public static long? GetInt64Ex(this DbDataReader reader, string columnName)
    {
        if (reader[columnName] == DBNull.Value) return null;

        return reader.GetInt64(columnName);
    }

    public static int? GetInt32Ex(this DbDataReader reader, string columnName)
    {
        if (reader[columnName] == DBNull.Value) return null;

        return reader.GetInt32(columnName);
    }

    public static DateTime? GetDateTimeEx(this DbDataReader reader, string columnName)
    {
        if (reader[columnName] == DBNull.Value) return null;

        return reader.GetDateTime(columnName);
    }

    public static TimeSpan? GetTimeSpanEx(this DbDataReader reader, string columnName)
    {
        if (reader[columnName] == DBNull.Value) return null;

        return reader.GetTimeSpan(columnName);
    }
}