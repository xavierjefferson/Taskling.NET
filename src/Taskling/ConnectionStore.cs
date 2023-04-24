using System.Collections.Generic;
using Taskling.InfrastructureContracts;
using Taskling.Tasks;

namespace Taskling;

public class ConnectionStore
{
    private readonly Dictionary<TaskId, ClientConnectionSettings> _connections;

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

    #region .: Singleton code :.

    private static volatile ConnectionStore _instance;
    private static readonly object sync = new();

    private ConnectionStore()
    {
        _connections = new Dictionary<TaskId, ClientConnectionSettings>();
    }

    public static ConnectionStore Instance
    {
        get
        {
            if (_instance == null)
                lock (sync)
                {
                    if (_instance == null) _instance = new ConnectionStore();
                }

            return _instance;
        }
    }

    #endregion .: Singleton code :.
}