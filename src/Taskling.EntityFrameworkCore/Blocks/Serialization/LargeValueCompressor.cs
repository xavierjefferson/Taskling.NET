using System.IO.Compression;
using System.Text;

namespace Taskling.EntityFrameworkCore.Blocks.Serialization;

public class LargeValueCompressor
{
    public static byte[] Zip(string? str)
    {
        var bytes = Encoding.UTF8.GetBytes(str);

        MemoryStream originalMemoryStream = null;
        MemoryStream compressedMemoryStream = null;

        try
        {
            originalMemoryStream = new MemoryStream(bytes);
            compressedMemoryStream = new MemoryStream();
            using (var compressionStream = new GZipStream(compressedMemoryStream, CompressionMode.Compress))
            {
                CopyStream(originalMemoryStream, compressionStream);
                //originalMemoryStream.CopyTo(compressionStream);
            }

            return compressedMemoryStream.ToArray();
        }
        finally
        {
            if (originalMemoryStream != null)
                originalMemoryStream.Dispose();
        }
    }

    public static string Unzip(byte[] bytes)
    {
        MemoryStream originalMemoryStream = null;
        MemoryStream decompressedMemoryStream = null;

        try
        {
            originalMemoryStream = new MemoryStream(bytes);
            decompressedMemoryStream = new MemoryStream();
            using (var decompressionStream = new GZipStream(originalMemoryStream, CompressionMode.Decompress))
            {
                decompressionStream.CopyTo(decompressedMemoryStream);
            }

            return Encoding.UTF8.GetString(decompressedMemoryStream.ToArray());
        }
        finally
        {
            if (decompressedMemoryStream != null)
                decompressedMemoryStream.Dispose();
        }
    }

    private static void CopyStream(Stream input, Stream output)
    {
        var buffer = new byte[4096];
        int read;
        while ((read = input.Read(buffer, 0, buffer.Length)) > 0) output.Write(buffer, 0, read);
    }
}