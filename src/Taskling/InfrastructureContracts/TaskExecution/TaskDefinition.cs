namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskDefinition
{
    public long TaskDefinitionId { get; set; }
    public string ApplicationName { get; set; }
    public string TaskName { get; set; }
}