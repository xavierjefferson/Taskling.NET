using System;
using Taskling.Enums;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.CriticalSections;

public class StartCriticalSectionRequest : RequestBase
{
    public StartCriticalSectionRequest(TaskId taskId,
        long taskExecutionId,
        TaskDeathModeEnum taskDeathMode,
        CriticalSectionTypeEnum criticalSectionType)
        : base(taskId, taskExecutionId)
    {
        TaskDeathMode = taskDeathMode;
        Type = criticalSectionType;
    }

    public CriticalSectionTypeEnum Type { get; set; }
    public TaskDeathModeEnum TaskDeathMode { get; set; }
    public TimeSpan? OverrideThreshold { get; set; }
    public TimeSpan? KeepAliveDeathThreshold { get; set; }
}