using System.Data.SqlClient;
using Taskling.Exceptions;
using Taskling.Serialization;

namespace Taskling.SqlServer.Blocks.Serialization;

public class SerializedValueReader
{
    public static T ReadValue<T>(string value, byte[] comp)
    {
        if (value == null && comp == null) return default;

        if (value != null) return JsonGenericSerializer.Deserialize<T>(value);

        if (comp != null)
        {
            var compressedBytes = comp;
            var uncompressedText = LargeValueCompressor.Unzip(compressedBytes);
            return JsonGenericSerializer.Deserialize<T>(uncompressedText);
        }

        throw new ExecutionException("The stored value is null which is not a valid state");
    }

    public static T ReadValue<T>(SqlDataReader reader, string valueColumn, string compressedColumn)
    {
        var value = reader[valueColumn];
        if (value == DBNull.Value && reader[compressedColumn] == DBNull.Value) return default;

        if (value != DBNull.Value) return JsonGenericSerializer.Deserialize<T>(value.ToString());

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
        var value = reader[valueColumn];
        if (value == DBNull.Value && reader[compressedColumn] == DBNull.Value) return string.Empty;

        if (value != DBNull.Value) return value.ToString();

        if (reader[compressedColumn] != DBNull.Value)
        {
            var compressedBytes = (byte[])reader[compressedColumn];
            var uncompressedText = LargeValueCompressor.Unzip(compressedBytes);
            return uncompressedText;
        }

        throw new ExecutionException("The stored value is null which is not a valid state");
    }

    public static string ReadValueAsString<T>(T item, Func<T, string> valueColumn, Func<T, byte[]> compressedColumn)
    {
        var value = valueColumn(item);
        var comp = compressedColumn(item);
        if (value == null && comp == null) return string.Empty;

        if (value != null) return value;

        if (comp != null)
        {
            var compressedBytes = comp;
            var uncompressedText = LargeValueCompressor.Unzip(compressedBytes);
            return uncompressedText;
        }

        throw new ExecutionException("The stored value is null which is not a valid state");
    }
}