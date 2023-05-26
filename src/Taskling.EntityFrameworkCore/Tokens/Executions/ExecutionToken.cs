using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Taskling.EntityFrameworkCore.Tokens.Executions;

public class ExecutionToken
{
    public ExecutionToken(ExecutionTokenStatus status, int grantedTaskExecutionId)
    {
        Status = status;
        GrantedTaskExecutionId = grantedTaskExecutionId;
    }

    public ExecutionToken()
    {
    }

    [JsonProperty("I")] public Guid TokenId { get; set; } = Guid.NewGuid();

    [JsonProperty("S")] public ExecutionTokenStatus Status { get; set; }

    [JsonProperty("G")] public long GrantedTaskExecutionId { get; set; }
}

public interface IExecutionTokenHelper
{
    ExecutionToken Create(ExecutionTokenStatus status, long g = 0);
    void SetStatus(ExecutionToken execution, ExecutionTokenStatus status);
    void SetGrantedToExecution(ExecutionToken execution, long g);
}

public class ExecutionTokenHelper : IExecutionTokenHelper
{
    private readonly ILogger<ExecutionTokenHelper> _logger;

    public ExecutionTokenHelper(ILogger<ExecutionTokenHelper> logger)
    {
        _logger = logger;
    }

    public ExecutionToken Create(ExecutionTokenStatus status, long g = 0)
    {
        return new ExecutionToken { Status = status, GrantedTaskExecutionId = g };
    }

    public void SetStatus(ExecutionToken execution, ExecutionTokenStatus status)
    {
        _logger.LogDebug($"Setting token status to {status}");
        execution.Status = status;
    }

    public void SetGrantedToExecution(ExecutionToken execution, long g)
    {
        _logger.LogDebug($"Setting token g to {g}");
        execution.GrantedTaskExecutionId = g;
    }
}