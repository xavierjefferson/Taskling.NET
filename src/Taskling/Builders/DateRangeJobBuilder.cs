using System;
using System.Threading.Tasks;
using Taskling.Contexts;

namespace Taskling.Builders;

public class DateRangeJobBuilder : JobBuilderBase<IDateRangeBlockContext, DateRangeJob, DateRangeJobBuilder>
{
    protected override DateRangeJobBuilder BuilderInstance => this;

    public override DateRangeJob Build()
    {
        return new DateRangeJob(_client, _application, _taskName, _getBlocksFunc, _processFunc,
            _failTaskOnException);
    }

    public DateRangeJobBuilder WithRange(Func<ITaskExecutionContext, Task<DateRange>> rangeFunc)
    {
        _getBlocksFunc = async taskExecutionContext =>
        {
            var range = await rangeFunc(taskExecutionContext);
            return await taskExecutionContext.GetDateRangeBlocksAsync(x =>
                x.WithRange(range.FromDate, range.ToDate, range.MaxBlockSize));
        };
        return this;
    }
}