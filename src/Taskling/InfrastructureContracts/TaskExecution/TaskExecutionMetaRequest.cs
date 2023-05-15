namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskExecutionMetaRequest : RequestBase
{
    public TaskExecutionMetaRequest(TaskId taskId) : base(taskId)
    {
    }

    public int ExecutionsToRetrieve { get; set; }
}