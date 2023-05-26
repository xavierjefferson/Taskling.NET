using Taskling.Enums;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.CriticalSections;

public class CompleteCriticalSectionRequest : RequestBase
{
    public CompleteCriticalSectionRequest(TaskId taskId, long taskExecutionId,
        CriticalSectionTypeEnum criticalSectionType)
        : base(taskId, taskExecutionId)
    {
        Type = criticalSectionType;
    }

    public CriticalSectionTypeEnum Type { get; set; }
}