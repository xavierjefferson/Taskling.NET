using Microsoft.Extensions.Logging;

namespace Taskling.Extensions;

public static class LoggerExtensions
{
    public static void Debug(this ILogger logger, string message)
    {
        logger.LogDebug(message);
    }
}