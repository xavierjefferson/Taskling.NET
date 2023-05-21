using System;

namespace Taskling;

public class TasklingOptions
{
    public TimeSpan CriticalSectionRetry { get; set; } = new(0, 0, 20);
    public int CriticalSectionAttemptCount { get; set; } = 3;
}