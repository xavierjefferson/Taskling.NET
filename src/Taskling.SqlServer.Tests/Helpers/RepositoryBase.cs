using System;
using Microsoft.EntityFrameworkCore;
using Taskling.SqlServer.Models;
using Taskling.SqlServer.Tests.Enums;

namespace Taskling.SqlServer.Tests.Helpers;

public abstract class RepositoryBase
{
    public TasklingDbContext GetDbContext()
    {
        return DbContextOptionsHelper.GetDbContext();
    }
}