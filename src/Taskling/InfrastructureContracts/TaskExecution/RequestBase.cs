namespace Taskling.InfrastructureContracts.TaskExecution;

public class RequestBase
{
    public RequestBase()
    {
    }

    public RequestBase(TaskId taskId)
    {
        TaskId = taskId;
    }

    public RequestBase(TaskId taskId, int taskExecutionId)
    {
        TaskId = taskId;
        TaskExecutionId = taskExecutionId;
    }

    public TaskId TaskId { get; set; }
    public int TaskExecutionId { get; set; }
}