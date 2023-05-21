using System.Reflection;
using Microsoft.Extensions.Logging;
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
    {
        _logger = logger;
        ConnectionStore = connectionStore;
        _dbContextFactoryEx = dbContextFactoryEx;
    }

    protected IConnectionStore ConnectionStore { get; }

    protected async Task<TasklingDbContext> GetDbContextAsync(TaskId? taskId)
    {
        await Task.CompletedTask;
        return _dbContextFactoryEx.GetDbContext(taskId);
    }
}