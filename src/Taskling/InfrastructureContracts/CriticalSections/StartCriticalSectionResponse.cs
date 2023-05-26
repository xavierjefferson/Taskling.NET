using Taskling.Enums;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.CriticalSections;

public class StartCriticalSectionResponse : ResponseBase
{
    public GrantStatusEnum GrantStatus { get; set; }
}