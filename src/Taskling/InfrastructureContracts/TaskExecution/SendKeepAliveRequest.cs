using System;

namespace Taskling.InfrastructureContracts.TaskExecution;

public class SendKeepAliveRequest : RequestBase
{
    public Guid ExecutionTokenId { get; set; }
}