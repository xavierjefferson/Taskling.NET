using Microsoft.Extensions.Logging;
using System.Reflection;
using Taskling.Extensions;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.Models;

namespace Taskling.SqlServer.AncilliaryServices;

public interface IDbContextFactoryEx
{
    TasklingDbContext GetDbContext(TaskId? taskId);
}

public abstract class DbOperationsService
{
    private readonly IDbContextFactoryEx _dbContextFactoryEx;
    private readonly ILogger<DbOperationsService> _logger;

    public DbOperationsService(IConnectionStore connectionStore, IDbContextFactoryEx dbContextFactoryEx,
        ILogger<DbOperationsService> logger)
    {    _logger = logger;
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        ConnectionStore = connectionStore;
        _dbContextFactoryEx = dbContextFactoryEx;
    
    }

    protected IConnectionStore ConnectionStore { get; }

    protected async Task<TasklingDbContext> GetDbContextAsync(TaskId? taskId)
    {

        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("859976b7-f185-4ab5-8e34-faa680aa93b8");
        await Task.CompletedTask;
        return _dbContextFactoryEx.GetDbContext(taskId);
    }
}