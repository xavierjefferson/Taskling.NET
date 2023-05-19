using System.Reflection;

namespace Taskling;

public static class Constants
{
    public const string CheckpointName = "Checkpoint";

    public static string GetEnteredMessage(MethodBase? method)
    {
        return $"Entered {method?.Name}";
    }
}