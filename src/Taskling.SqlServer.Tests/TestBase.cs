using System;
using System.Threading;
using System.Threading.Tasks;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.Tasks;
using Taskling.SqlServer.Tests.Helpers;
using Xunit;

namespace Taskling.SqlServer.Tests;

public abstract class TestBase
{
    private static readonly SemaphoreSlim Semaphore = new(1);

    public TestBase(IExecutionsHelper executionsHelper)
    {
        CurrentTaskId = executionsHelper.CurrentTaskId;
    }

    public object MyLock { get; } = new();

    protected TaskId CurrentTaskId { get; }

    public static void AssertSimilarDates(DateTime d1, DateTime d2)
    {
        Assert.True(Math.Abs(d2.Subtract(d2).TotalSeconds) < 1);
    }

    protected void InSemaphore(Action action)
    {
        Semaphore.Wrap(action);
    }

    protected async Task InSemaphoreAsync(Func<Task> func)
    {
        await Semaphore.WrapAsync(func);
    }
}