using Microsoft.Extensions.DependencyInjection;
using Taskling.Configuration;
using Taskling.EntityFrameworkCore.AncilliaryServices;

namespace Taskling.EntityFrameworkCore.Builders;

public class TasklingServiceOptionsBuilder
{
    private readonly IServiceCollection _serviceCollection;

    private IDbContextConfigurator? _dbContextSelector;

    //private Action<TasklingTaskConfigurationBuilder> func;
    public TasklingServiceOptionsBuilder(IServiceCollection serviceCollection)
    {
        _serviceCollection = serviceCollection;
    }

    public TasklingServiceOptionsBuilder WithReader<T>() where T : class, ITaskConfigurationReader
    {
        _serviceCollection.AddSingleton<ITaskConfigurationReader, T>();
        return this;
    }


    public TasklingServiceOptionsBuilder WithDbContextOptions(Action<TasklingDbContextEventArgs> func)
    {
        _dbContextSelector = new DelegatingDbContextConfigurator(func);
        return this;
    }

    public void Build()
    {
        if (_dbContextSelector == null) throw new InvalidOperationException("Must set dbcontextselector");
        _serviceCollection.AddSingleton(_dbContextSelector);
        _serviceCollection.AddSingleton<IDbContextFactoryEx, DbContextFactoryEx>();
    }
}