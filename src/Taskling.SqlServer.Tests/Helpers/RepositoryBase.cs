using System;
using Microsoft.EntityFrameworkCore;
using Taskling.SqlServer.Models;
using Taskling.SqlServer.Tests.Enums;

namespace Taskling.SqlServer.Tests.Helpers;

public abstract class RepositoryBase
{
    private static bool created;
    private static readonly object _mutex = new();

    public TasklingDbContext GetDbContext()
    {
        lock (_mutex)
        {
            var builder = new DbContextOptionsBuilder<TasklingDbContext>();

            //builder.UseSqlServer(TestConstants.TestConnectionString);
            switch (TestConstants.ConnectionType)
            {
                case ConnectionTypeEnum.MySql:
                    builder.UseMySQL(TestConstants.GetTestConnectionString());
                    break;
                case ConnectionTypeEnum.SqlServer:
                    builder.UseSqlServer(TestConstants.GetTestConnectionString());
                    break;
                default: throw new NotImplementedException();
            }


            var tasklingDbContext = new TasklingDbContext(builder.Options);

            if (!created)
            {
                tasklingDbContext.Database.EnsureCreated();
                created = true;
            }

            return tasklingDbContext;
        }
    }
}