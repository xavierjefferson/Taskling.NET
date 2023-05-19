using Newtonsoft.Json;
using System.Reflection;
using Microsoft.Extensions.Logging;
using Taskling.Extensions;

namespace Taskling.SqlServer.Tokens.Executions;

public class ExecutionToken
{
    [JsonProperty("I")] public Guid TokenId { get; set; } = Guid.NewGuid();

    [JsonProperty("S")] public ExecutionTokenStatus Status { get; set; }

    [JsonProperty("G")] public int GrantedToExecution { get; set; }
}

public interface IExecutionTokenHelper
{
    ExecutionToken Create(ExecutionTokenStatus status, int g = 0);
    void SetStatus(ExecutionToken execution, ExecutionTokenStatus status);
    void SetGrantedToExecution(ExecutionToken execution, int g);
}

public class ExecutionTokenHelper : IExecutionTokenHelper
{
    private readonly ILogger<ExecutionTokenHelper> _logger;

    public ExecutionTokenHelper(ILogger<ExecutionTokenHelper> logger)
    {
        _logger = logger;
    }

    public  ExecutionToken Create(ExecutionTokenStatus status, int g = 0)
    {
        _logger.Debug("c9c4b7cb-7ea9-4d8f-abd4-4c011e481210");
        return new ExecutionToken() { Status = status, GrantedToExecution = g };
    }

    public   void SetStatus(ExecutionToken execution, ExecutionTokenStatus status)
    {
        _logger.Debug("f02077dd-8364-4607-a1ba-0913d81ed783");
        _logger.Debug($"Setting token status to {status}");
        execution.Status = status;
    }

    public   void SetGrantedToExecution(ExecutionToken execution, int g)
    {
        _logger.Debug("ca65a673-5d53-4e93-afc1-feb23a86383e");
        _logger.Debug($"Setting token g to {g}");
        execution.GrantedToExecution = g;
    }
}