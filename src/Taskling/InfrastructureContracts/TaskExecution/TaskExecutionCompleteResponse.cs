using System;

namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskExecutionCompleteResponse : ResponseBase
{
    public DateTime CompletedAt { get; set; }
}