using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Taskling.Builders;
using Taskling.Contexts;

namespace Taskling.Extensions;

public static class clientHelper
{
    public static async Task StartNumberRange(this ITasklingClient client, string applicationName, string taskName, Func<ITaskExecutionContext, Task<NumericRange>> rangeFunc, 
        Func<INumericRangeBlockContext, Task> processFunc)
    {
        var p = new NumericRangeJobBuilder().WithClient(client).WithApplication(applicationName).WithTaskName(taskName)
            .WithRange(rangeFunc).WithProcessFunc(processFunc);

        await p.Build().Execute();
    }

    public static async Task StartDateRange(this ITasklingClient client, string applicationName, string taskName, Func<ITaskExecutionContext, Task<DateRange>> rangeFunc,
        Func<IDateRangeBlockContext, Task> processFunc)
    {
        var p = new DateRangeJobBuilder().WithClient(client).WithApplication(applicationName).WithTaskName(taskName)
            .WithRange(rangeFunc).WithProcessFunc(processFunc);

        await p.Build().Execute();
       
    }

}