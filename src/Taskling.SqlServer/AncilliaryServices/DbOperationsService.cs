using Taskling.InfrastructureContracts;
using Taskling.SqlServer.Models;

namespace Taskling.SqlServer.AncilliaryServices;

public interface IDbContextFactoryEx
{
    TasklingDbContext GetDbContext(TaskId taskId);
}

public abstract class DbOperationsService
{
    private readonly IDbContextFactoryEx _dbContextFactoryEx;

    public DbOperationsService(IConnectionStore connectionStore, IDbContextFactoryEx dbContextFactoryEx)
    {
        ConnectionStore = connectionStore;
        _dbContextFactoryEx = dbContextFactoryEx;
    }

    protected IConnectionStore ConnectionStore { get; }

    protected async Task<TasklingDbContext> GetDbContextAsync(TaskId taskId)
    {
        await Task.CompletedTask;
        return _dbContextFactoryEx.GetDbContext(taskId);
    }
}