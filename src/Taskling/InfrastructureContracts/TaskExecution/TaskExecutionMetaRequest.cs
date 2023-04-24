namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskExecutionMetaRequest : RequestBase
{
    public int ExecutionsToRetrieve { get; set; }
}