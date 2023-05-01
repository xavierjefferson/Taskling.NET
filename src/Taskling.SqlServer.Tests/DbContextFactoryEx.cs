using System;
using Microsoft.EntityFrameworkCore;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Models;
using Taskling.SqlServer.Tests.Enums;
using Taskling.SqlServer.Tests.Helpers;

namespace Taskling.SqlServer.Tests;

public class DbContextFactoryEx : IDbContextFactoryEx
{
    private readonly IConnectionStore _connectionStore;

    public DbContextFactoryEx(IConnectionStore connectionStore)
    {
        _connectionStore = connectionStore;
    }

    public TasklingDbContext GetDbContext(TaskId taskId)
    {
        var builder = new DbContextOptionsBuilder<TasklingDbContext>();
        var clientConnectionSettings = _connectionStore.GetConnection(taskId);
        switch (TestConstants.ConnectionType)
        {
            case ConnectionTypeEnum.SqlServer:
                builder.UseSqlServer(clientConnectionSettings.ConnectionString, options =>
                {
                    options.CommandTimeout(clientConnectionSettings.QueryTimeoutSeconds);

                    //       options.EnableRetryOnFailure();
                });
                break;
            case ConnectionTypeEnum.MySql:
                builder.UseMySQL(clientConnectionSettings.ConnectionString,
                    options => { options.CommandTimeout(clientConnectionSettings.QueryTimeoutSeconds); });
                break;
            default: throw new NotImplementedException();
        }


        var tasklingDbContext = new TasklingDbContext(builder.Options);
        tasklingDbContext.Database.SetCommandTimeout(clientConnectionSettings.QueryTimeout);
        return tasklingDbContext;
    }
}