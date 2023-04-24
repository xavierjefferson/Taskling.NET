namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskExecutionErrorRequest : RequestBase
{
    public string Error { get; set; }
    public bool TreatTaskAsFailed { get; set; }
}