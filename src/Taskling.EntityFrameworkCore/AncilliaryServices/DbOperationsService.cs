using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.Models;
using Taskling.InfrastructureContracts;

namespace Taskling.EntityFrameworkCore.AncilliaryServices;

public interface IDbContextFactoryEx
{
    TasklingDbContext GetDbContext(TaskId? taskId);
}

public abstract class DbOperationsService
{
    private readonly IDbContextFactoryEx _dbContextFactoryEx;
    private readonly ILogger<DbOperationsService> _logger;

    public DbOperationsService(IDbContextFactoryEx dbContextFactoryEx,
        ILogger<DbOperationsService> logger)
    {
        _logger = logger;
        _dbContextFactoryEx = dbContextFactoryEx;
    }

    protected async Task<TasklingDbContext> GetDbContextAsync(TaskId? taskId)
    {
        await Task.CompletedTask;
        return _dbContextFactoryEx.GetDbContext(taskId);
    }
}