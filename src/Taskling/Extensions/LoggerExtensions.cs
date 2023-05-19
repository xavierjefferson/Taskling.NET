using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Taskling.Extensions
{
    public static class LoggerExtensions
    {
        public static void Debug(this ILogger logger, string message)
        {
            logger.LogDebug(message);
        }
    }
}
