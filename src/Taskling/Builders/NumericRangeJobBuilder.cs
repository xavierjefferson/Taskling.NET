using System;
using System.Threading.Tasks;
using Taskling.Contexts;

namespace Taskling.Builders;

public class NumericRangeJobBuilder : JobBuilderBase<INumericRangeBlockContext, NumericRangeJob, NumericRangeJobBuilder>
{
    protected override NumericRangeJobBuilder BuilderInstance => this;


    public override NumericRangeJob Build()
    {
        return new NumericRangeJob(_client, _application, _taskName, _getBlocksFunc, _processFunc,
            _failTaskOnException);
    }


    public NumericRangeJobBuilder WithRange(Func<ITaskExecutionContext, Task<NumericRange>> rangeFunc)
    {
        _getBlocksFunc = async taskExecutionContext =>
        {
            var range = await rangeFunc(taskExecutionContext);
            return await taskExecutionContext.GetNumericRangeBlocksAsync(x =>
                x.WithRange(range.FromNumber, range.ToNumber, range.MaxBlockSize));
        };
        return this;
    }
}