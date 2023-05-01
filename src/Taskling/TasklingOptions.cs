using System;

namespace Taskling;

public class TasklingOptions
{
    public TimeSpan CriticalSectionRetry { get; set; } = TimeSpan.FromMinutes(10);
    public int CriticalSectionAttemptCount { get; set; } = 100;
}