using Taskling.Exceptions;
using Taskling.Serialization;

namespace Taskling.SqlServer.Blocks.Serialization;

public class SerializedValueReader
{
    public static T ReadValue<T>(string? value, byte[]? compressedBytes)
    {
        if (value == null && compressedBytes == null) return default;

        if (value != null) return JsonGenericSerializer.Deserialize<T>(value);

        if (compressedBytes != null)
        {
            var uncompressedText = LargeValueCompressor.Unzip(compressedBytes);
            return JsonGenericSerializer.Deserialize<T>(uncompressedText);
        }

        throw new ExecutionException("The stored value is null which is not a valid state");
    }

    public static string ReadValueAsString(string? value, byte[]? compressedBytes)
    {
        if (value == null && compressedBytes == null) return string.Empty;

        if (value != null) return value;

        if (compressedBytes != null)
        {
             
            var uncompressedText = LargeValueCompressor.Unzip(compressedBytes);
            return uncompressedText;
        }

        throw new ExecutionException("The stored value is null which is not a valid state");
    }
}