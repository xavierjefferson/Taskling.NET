using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Taskling.Contexts;

namespace Taskling.Builders;

public class DateRangeJob : JobBase<IDateRangeBlockContext>
{
    public DateRangeJob(ITasklingClient tasklingClient, string applicationName, string taskName,
        Func<ITaskExecutionContext, Task<IList<IDateRangeBlockContext>>> getBlockFunc,
        Func<IDateRangeBlockContext, Task> processFunc, bool exceptionCausesFailedTask) : base(tasklingClient,
        applicationName, taskName, getBlockFunc, processFunc, exceptionCausesFailedTask)
    {
    }
}