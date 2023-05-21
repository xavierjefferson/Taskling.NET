using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.CriticalSections;

public class CompleteCriticalSectionRequest : RequestBase
{
    public CompleteCriticalSectionRequest(TaskId taskId, long taskExecutionId, CriticalSectionType criticalSectionType)
        : base(taskId, taskExecutionId)
    {
        Type = criticalSectionType;
    }

    public CriticalSectionType Type { get; set; }
}