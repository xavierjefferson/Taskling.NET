using System;

namespace Taskling.InfrastructureContracts.TaskExecution;

public class SendKeepAliveRequest : RequestBase
{
    public SendKeepAliveRequest(TaskId taskId) : base(taskId)
    {
    }

    public Guid ExecutionTokenId { get; set; }
}