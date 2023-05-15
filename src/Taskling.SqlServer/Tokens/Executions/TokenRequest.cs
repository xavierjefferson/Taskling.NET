using Taskling.InfrastructureContracts;

namespace Taskling.SqlServer.Tokens.Executions;

public class TokenRequest
{
    public TaskId? TaskId { get; set; }
    public int TaskDefinitionId { get; set; }
    public int TaskExecutionId { get; set; }
    public int ConcurrencyLimit { get; set; }
}