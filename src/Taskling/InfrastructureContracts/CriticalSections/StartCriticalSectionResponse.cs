using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.CriticalSections;

public class StartCriticalSectionResponse : ResponseBase
{
    public GrantStatus GrantStatus { get; set; }
}