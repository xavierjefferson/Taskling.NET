namespace Taskling.SqlServer.Tokens.Executions;

public class ExecutionTokenList
{
    public ExecutionTokenList()
    {
        Tokens = new List<ExecutionToken>();
    }

    public List<ExecutionToken> Tokens { get; set; }
}