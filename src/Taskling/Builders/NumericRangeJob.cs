using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Taskling.Contexts;

namespace Taskling.Builders;

public class NumericRangeJob : JobBase<INumericRangeBlockContext>
{
    public NumericRangeJob(ITasklingClient tasklingClient, string applicationName, string taskName,
        Func<ITaskExecutionContext, Task<IList<INumericRangeBlockContext>>> getBlockFunc,
        Func<INumericRangeBlockContext, Task> processFunc, bool exceptionCausesFailedTask) : base(tasklingClient,
        applicationName, taskName, getBlockFunc, processFunc, exceptionCausesFailedTask)
    {
    }
}