namespace Taskling.InfrastructureContracts.TaskExecution;

public abstract class RequestBase
{
    public RequestBase(TaskId taskId, int taskExecutionId = 0)
    {
        TaskId = taskId;
        TaskExecutionId = taskExecutionId;
    }

    public TaskId TaskId { get; }
    public int TaskExecutionId { get; set; }
}