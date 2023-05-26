using Taskling.Enums;

namespace Taskling.EntityFrameworkCore.Tokens.Executions;

public class TokenResponse
{
    public Guid ExecutionTokenId { get; set; }
    public DateTime StartedAt { get; set; }
    public GrantStatusEnum GrantStatus { get; set; }
}