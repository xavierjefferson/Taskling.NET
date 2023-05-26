using System;
using Taskling.Enums;

namespace Taskling.Tasks;

public class TaskExecutionOptions
{
    public TaskDeathModeEnum TaskDeathMode { get; set; }
    public TimeSpan? OverrideThreshold { get; set; }
    public TimeSpan? KeepAliveInterval { get; set; }
    public TimeSpan? KeepAliveDeathThreshold { get; set; }
    public int ConcurrencyLimit { get; set; }
    public bool Enabled { get; set; }
}