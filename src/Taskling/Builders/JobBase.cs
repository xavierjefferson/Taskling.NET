using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Taskling.Contexts;

namespace Taskling.Builders;

public abstract class JobBase<T>
{
    private readonly string _applicationName;
    private readonly bool _exceptionCausesFailedTask;
    private readonly Func<ITaskExecutionContext, Task<IList<T>>> _getBlockFunc;
    private readonly Func<T, Task> _processFunc;
    private readonly ITasklingClient _tasklingClient;
    private readonly string _taskName;

    public JobBase(ITasklingClient tasklingClient, string applicationName, string taskName,
        Func<ITaskExecutionContext, Task<IList<T>>> getBlockFunc,
        Func<T, Task> processFunc, bool exceptionCausesFailedTask)
    {
        _tasklingClient = tasklingClient;
        _applicationName = applicationName;
        _taskName = taskName;
        _getBlockFunc = getBlockFunc;
        _processFunc = processFunc;
        _exceptionCausesFailedTask = exceptionCausesFailedTask;
    }

    public async Task Execute()
    {
        using (var taskExecutionContext =
               _tasklingClient.CreateTaskExecutionContext(_applicationName, _taskName))
        {
            if (await taskExecutionContext.TryStartAsync())
                using (var criticalSection = taskExecutionContext.CreateCriticalSection())
                {
                    if (await criticalSection.TryStartAsync())
                    {
                        var context = await _getBlockFunc(taskExecutionContext);
                        try
                        {
                            foreach (var block in context)
                                await _processFunc(block);

                            await taskExecutionContext.CompleteAsync();
                        }
                        catch (Exception ex)
                        {
                            await taskExecutionContext.ErrorAsync(ex.ToString(), _exceptionCausesFailedTask);
                        }
                    }

                    throw new Exception("Could not acquire a critical section, aborted task");
                }
        }
    }
}