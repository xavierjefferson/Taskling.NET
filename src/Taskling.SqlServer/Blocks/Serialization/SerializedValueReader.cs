using System.Data.SqlClient;
using Taskling.Exceptions;
using Taskling.Serialization;

namespace Taskling.SqlServer.Blocks.Serialization;

public class SerializedValueReader
{
    public static T ReadValue<T>(SqlDataReader reader, string valueColumn, string compressedColumn)
    {
        if (reader[valueColumn] == DBNull.Value && reader[compressedColumn] == DBNull.Value) return default;

        if (reader[valueColumn] != DBNull.Value)
        {
            return JsonGenericSerializer.Deserialize<T>(reader[valueColumn].ToString());
        }

        if (reader[compressedColumn] != DBNull.Value)
        {
            var compressedBytes = (byte[])reader[compressedColumn];
            var uncompressedText = LargeValueCompressor.Unzip(compressedBytes);
            return JsonGenericSerializer.Deserialize<T>(uncompressedText);
        }

        throw new ExecutionException("The stored value is null which is not a valid state");
    }

    public static string ReadValueAsString(SqlDataReader reader, string valueColumn, string compressedColumn)
    {
        if (reader[valueColumn] == DBNull.Value && reader[compressedColumn] == DBNull.Value) return string.Empty;

        if (reader[valueColumn] != DBNull.Value)
        {
            return reader[valueColumn].ToString();
        }

        if (reader[compressedColumn] != DBNull.Value)
        {
            var compressedBytes = (byte[])reader[compressedColumn];
            var uncompressedText = LargeValueCompressor.Unzip(compressedBytes);
            return uncompressedText;
        }

        throw new ExecutionException("The stored value is null which is not a valid state");
    }
}