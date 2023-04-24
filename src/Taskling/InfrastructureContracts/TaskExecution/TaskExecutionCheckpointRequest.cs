namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskExecutionCheckpointRequest : RequestBase
{
    public string Message { get; set; }
}