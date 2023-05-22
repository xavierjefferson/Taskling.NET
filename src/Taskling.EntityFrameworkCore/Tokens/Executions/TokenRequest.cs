using Taskling.InfrastructureContracts;

namespace Taskling.EntityFrameworkCore.Tokens.Executions;

public class TokenRequest
{
    public TaskId? TaskId { get; set; }
    public long TaskDefinitionId { get; set; }
    public long TaskExecutionId { get; set; }
    public int ConcurrencyLimit { get; set; }
}