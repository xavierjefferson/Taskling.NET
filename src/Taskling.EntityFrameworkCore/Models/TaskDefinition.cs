namespace Taskling.EntityFrameworkCore.Models;

public class TaskDefinition
{
    public TaskDefinition()
    {
        Blocks = new HashSet<Block>();
        TaskExecutions = new HashSet<TaskExecution>();
    }

    public long TaskDefinitionId { get; set; }
    public string? ApplicationName { get; set; }
    public string? TaskName { get; set; }
    public DateTime? LastCleaned { get; set; }
    public string? ExecutionTokens { get; set; }
    public int UserCsStatus { get; set; }
    public long? UserCsTaskExecutionId { get; set; }
    public string? UserCsQueue { get; set; }
    public int ClientCsStatus { get; set; }
    public long? ClientCsTaskExecutionId { get; set; }
    public string? ClientCsQueue { get; set; }
    public long? HoldLockTaskExecutionId { get; set; }

    public virtual ICollection<Block> Blocks { get; set; }
    public virtual ICollection<TaskExecution> TaskExecutions { get; set; }
}