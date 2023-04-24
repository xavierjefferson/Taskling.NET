using System;

namespace Taskling.Tasks;

public class ClientConnectionSettings
{
    public ClientConnectionSettings(string connectionString, TimeSpan queryTimeout)
    {
        ConnectionString = connectionString;
        QueryTimeout = queryTimeout;
    }

    public string ConnectionString { get; set; }
    public TimeSpan QueryTimeout { get; set; }

    public int QueryTimeoutSeconds => (int)QueryTimeout.TotalSeconds;
}