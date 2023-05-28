using Microsoft.EntityFrameworkCore;
using Taskling.EntityFrameworkCore.Models;
using Taskling.InfrastructureContracts;

namespace Taskling.EntityFrameworkCore;

public interface IDbContextConfigurator
{
    public void Configure(TasklingDbContextEventArgs args);
}

public class DelegatingDbContextConfigurator : IDbContextConfigurator
{
    private readonly Action<TasklingDbContextEventArgs> _func;

    public DelegatingDbContextConfigurator(Action<TasklingDbContextEventArgs> func)
    {
        _func = func;
    }

    public void Configure(TasklingDbContextEventArgs args)
    {
        _func(args);
    }
}

public class TasklingDbContextEventArgs : EventArgs
{
    public TasklingDbContextEventArgs(DbContextOptionsBuilder<TasklingDbContext> builder, TaskId taskId,
        string connectionString)
    {
        Builder = builder;
        TaskId = taskId;
        ConnectionString = connectionString;
    }

    public DbContextOptionsBuilder<TasklingDbContext> Builder { get; }
    public TaskId TaskId { get; }
    public string ConnectionString { get; }
}