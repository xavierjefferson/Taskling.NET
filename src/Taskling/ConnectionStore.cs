using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Taskling.InfrastructureContracts;
using Taskling.Tasks;

namespace Taskling;

public class ConnectionStore : IConnectionStore
{
    //#region .: Singleton code :.

    //private static volatile ConnectionStore _instance;
    private static readonly object sync = new();
    private readonly Dictionary<TaskId, ClientConnectionSettings> _connections;
    private readonly ILogger<ConnectionStore> _logger;

    public ConnectionStore(ILogger<ConnectionStore> logger)
    {
        _logger = logger;
        _connections = new Dictionary<TaskId, ClientConnectionSettings>();
    }

    public void SetConnection(TaskId taskId, ClientConnectionSettings connectionSettings)
    {
        lock (sync)
        {
            if (_connections.ContainsKey(taskId))
                _connections[taskId] = connectionSettings;
            else
                _connections.Add(taskId, connectionSettings);
        }
    }

    public ClientConnectionSettings GetConnection(TaskId taskId)
    {
        lock (sync)
        {
            if (_connections.ContainsKey(taskId))
                return _connections[taskId];
            return null;
        }
    }
}