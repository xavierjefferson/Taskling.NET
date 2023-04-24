using System.Collections.Generic;

namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskExecutionMetaResponse
{
    public TaskExecutionMetaResponse()
    {
        Executions = new List<TaskExecutionMetaItem>();
    }

    public List<TaskExecutionMetaItem> Executions { get; set; }
}