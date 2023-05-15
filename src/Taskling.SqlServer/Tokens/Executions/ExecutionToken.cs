using Newtonsoft.Json;

namespace Taskling.SqlServer.Tokens.Executions;

public class ExecutionToken
{
    [JsonProperty("I")] public Guid TokenId { get; set; } = Guid.NewGuid();
    [JsonProperty("S")]
    public ExecutionTokenStatus Status { get; set; }
    [JsonProperty("G")]
    public int GrantedToExecution { get; set; }
}