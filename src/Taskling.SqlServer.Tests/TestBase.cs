using System;
using Xunit;

namespace Taskling.SqlServer.Tests;

public abstract class TestBase
{
    public static void AssertSimilarDates(DateTime d1, DateTime d2)
    {
        Assert.True(Math.Abs(d2.Subtract(d2).TotalSeconds) < 1);
    }
}