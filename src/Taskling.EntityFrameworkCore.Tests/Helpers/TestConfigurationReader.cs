using Microsoft.Extensions.Logging;
using Taskling.Configuration;
using Taskling.InfrastructureContracts;

namespace Taskling.EntityFrameworkCore.Tests.Helpers;

public class TestConfigurationReader : IConfigurationReader
{
    private readonly ConfigurationOptions _configurationOptions;
    private readonly ILogger<TestConfigurationReader> _logger;

    public TestConfigurationReader(ConfigurationOptions configurationOptions, ILogger<TestConfigurationReader> logger)
    {
        _logger = logger;
        _configurationOptions = configurationOptions;
    }

    public ConfigurationOptions GetTaskConfigurationString(TaskId taskId)
    {
        return
            _configurationOptions; // "DB(Server=(local);Database=TasklingDb;Trusted_Connection=True;) TO(120) E(true) CON(-1) KPLT(2) KPDT(40) MCI(1) KA(true) KAINT(1) KADT(10) TPDT(0) RPC_FAIL(true) RPC_FAIL_MTS(600) RPC_FAIL_RTYL(3) RPC_DEAD(true) RPC_DEAD_MTS(600) RPC_DEAD_RTYL(3) MXBL(20)";
    }
}