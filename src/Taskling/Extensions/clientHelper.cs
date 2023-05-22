using System;
using System.Threading.Tasks;
using Taskling.Builders;
using Taskling.Contexts;

namespace Taskling.Extensions;

public static class ClientHelper
{
    public static async Task StartNumberRange(this ITasklingClient client, string applicationName, string taskName,
        Func<ITaskExecutionContext, Task<NumericRange>> rangeFunc,
        Func<INumericRangeBlockContext, Task> processFunc)
    {
        var builder = new NumericRangeJobBuilder().WithClient(client).WithApplication(applicationName).WithTaskName(taskName)
            .WithRange(rangeFunc).WithProcessFunc(processFunc);

        await builder.Build().Execute();
    }
}