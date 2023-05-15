namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskExecutionErrorRequest : RequestBase
{
    public TaskExecutionErrorRequest(TaskId taskId) : base(taskId)
    {
    }

    public string Error { get; set; }
    public bool TreatTaskAsFailed { get; set; }
}