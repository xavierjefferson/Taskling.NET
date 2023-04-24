namespace Taskling.SqlServer.Tokens.Executions;

public class ExecutionToken
{
    public Guid TokenId { get; set; }
    public ExecutionTokenStatus Status { get; set; }
    public int GrantedToExecution { get; set; }
}