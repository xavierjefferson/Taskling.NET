using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.EntityFrameworkCore.Tokens.Executions;

public class TokenResponse
{
    public Guid ExecutionTokenId { get; set; }
    public DateTime StartedAt { get; set; }
    public GrantStatus GrantStatus { get; set; }
}