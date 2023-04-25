namespace Taskling.SqlServer.Models;

public class TaskDefinition
{
    public TaskDefinition()
    {
        Blocks = new HashSet<Block>();
        TaskExecutions = new HashSet<TaskExecution>();
    }

    public int TaskDefinitionId { get; set; }
    public string? ApplicationName { get; set; }
    public string? TaskName { get; set; }
    public DateTime? LastCleaned { get; set; }
    public string? ExecutionTokens { get; set; }
    public int UserCsStatus { get; set; }
    public int? UserCsTaskExecutionId { get; set; }
    public string? UserCsQueue { get; set; }
    public int ClientCsStatus { get; set; }
    public int? ClientCsTaskExecutionId { get; set; }
    public string? ClientCsQueue { get; set; }
    public int? HoldLockTaskExecutionId { get; set; }

    public virtual ICollection<Block> Blocks { get; set; }
    public virtual ICollection<TaskExecution> TaskExecutions { get; set; }
}