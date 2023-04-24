using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.SqlServer.Tokens.Executions;

public class TokenResponse
{
    public Guid ExecutionTokenId { get; set; }
    public DateTime StartedAt { get; set; }
    public GrantStatus GrantStatus { get; set; }
}