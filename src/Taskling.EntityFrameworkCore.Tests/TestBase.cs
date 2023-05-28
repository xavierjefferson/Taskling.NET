using System;
using System.Threading;
using System.Threading.Tasks;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Taskling.InfrastructureContracts;
using Xunit;

namespace Taskling.EntityFrameworkCore.Tests;

public abstract class TestBase
{
    private static readonly SemaphoreSlim Semaphore = new(1);

    public TestBase(IExecutionsHelper executionsHelper)
    {
        //_logger.LogDebug($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {Constants.CheckpointName}");
        CurrentTaskId = executionsHelper.CurrentTaskId;
    }

    public object MyLock { get; } = new();

    protected TaskId CurrentTaskId { get; }

    public static void AssertSimilarDates(DateTime d1, DateTime d2)
    {
        //_logger.LogDebug($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {Constants.CheckpointName}");
        Assert.True(Math.Abs(d2.Subtract(d2).TotalSeconds) < 1);
    }

    protected void InSemaphore(Action action)
    {
        //_logger.LogDebug($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {Constants.CheckpointName}");
        Semaphore.Wrap(action);
    }

    protected async Task InSemaphoreAsync(Func<Task> func)
    {
        //_logger.LogDebug($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {Constants.CheckpointName}");
        await Semaphore.WrapAsync(func);
    }

    public const string CollectionName = "DefaultCollection";
}