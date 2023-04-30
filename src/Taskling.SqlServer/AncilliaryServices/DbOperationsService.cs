using System.Transactions;
using Microsoft.EntityFrameworkCore;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.Models;

namespace Taskling.SqlServer.AncilliaryServices;

public abstract class DbOperationsService  
{
    protected IConnectionStore ConnectionStore { get; }

    public DbOperationsService(IConnectionStore connectionStore)
    {
        ConnectionStore = connectionStore;
    }
    protected async Task<TasklingDbContext> GetDbContextAsync(TaskId taskId)
    {
        var builder = new DbContextOptionsBuilder<TasklingDbContext>();
        var clientConnectionSettings = ConnectionStore.GetConnection(taskId);
        builder.UseSqlServer(clientConnectionSettings.ConnectionString, options =>
        {
            options.CommandTimeout(clientConnectionSettings.QueryTimeoutSeconds);
            
            //       options.EnableRetryOnFailure();
        });
        await Task.CompletedTask;

        var tasklingDbContext = new TasklingDbContext(builder.Options);
        tasklingDbContext.Database.SetCommandTimeout(clientConnectionSettings.QueryTimeout);
        return tasklingDbContext;
    }
}