namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskExecutionCheckpointRequest : RequestBase
{
    public TaskExecutionCheckpointRequest(TaskId taskId) : base(taskId)
    {
    }

    public string Message { get; set; }
}