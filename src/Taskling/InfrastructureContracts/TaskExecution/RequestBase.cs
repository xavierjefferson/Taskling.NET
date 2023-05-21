namespace Taskling.InfrastructureContracts.TaskExecution;

public abstract class RequestBase
{
    public RequestBase(TaskId taskId, long taskExecutionId = 0)
    {
        TaskId = taskId;
        TaskExecutionId = taskExecutionId;
    }

    public TaskId TaskId { get; }
    public long TaskExecutionId { get; set; }
}