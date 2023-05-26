using System;

namespace Taskling.EntityFrameworkCore.Tests;

public static class TimeSpans
{
    public static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan FourSeconds = TimeSpan.FromSeconds(4);
    public static readonly TimeSpan FiveSeconds = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan TwentySeconds = TimeSpan.FromSeconds(20);
    public static readonly TimeSpan OneMinute = TimeSpan.FromMinutes(1);
    public static readonly TimeSpan TenMinutes = TimeSpan.FromMinutes(10);
    public static readonly TimeSpan ThirtyMinutes = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan OneHour = TimeSpan.FromHours(1);
    public static readonly TimeSpan OneDay = TimeSpan.FromDays(1);
    public static readonly TimeSpan TwoDays = TimeSpan.FromDays(2);
}