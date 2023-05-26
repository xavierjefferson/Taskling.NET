using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Taskling.Contexts;

namespace Taskling.Builders;

public abstract class JobBuilderBase<T, TJob, TBuilder>
{
    protected string _application;
    protected ITasklingClient _client;
    protected bool _failTaskOnException = true;
    protected Func<ITaskExecutionContext, Task<IList<T>>> _getBlocksFunc;
    protected Func<T, Task> _processFunc;
    protected string _taskName;
    protected abstract TBuilder BuilderInstance { get; }
    public abstract TJob Build();

    public TBuilder WithRange(
        Func<ITaskExecutionContext, Task<IList<T>>> blockFunc)
    {
        _getBlocksFunc = async taskExecutionContext => await blockFunc(taskExecutionContext);
        return BuilderInstance;
    }

    public TBuilder WithExceptionCausesFailedTask(bool value)
    {
        _failTaskOnException = value;
        return BuilderInstance;
    }

    public TBuilder WithTaskName(string taskName)
    {
        _taskName = taskName;
        return BuilderInstance;
    }

    public TBuilder WithApplication(string application)
    {
        _application = application;
        return BuilderInstance;
    }

    public TBuilder WithProcessFunc(Func<T, Task> processFunc)
    {
        _processFunc = processFunc;
        return BuilderInstance;
    }

    public TBuilder WithClient(ITasklingClient client)
    {
        _client = client;
        return BuilderInstance;
    }
}