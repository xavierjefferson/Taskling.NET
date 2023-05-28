using System;

namespace Taskling.EntityFrameworkCore.Tests.Helpers;

public static class DateTimeHelper
{
   
    public static DateTime CreateUtcDate(int year, int month, int day)
    {
        return DateTime.SpecifyKind(new DateTime(year, month, day), DateTimeKind.Utc);
    }
    public static DateTime CreateUtcDate(int a, int b, int c, int d, int e, int f, int g)
    {
        return DateTime.SpecifyKind(new DateTime(a,b,c,d,e,f,g), DateTimeKind.Utc);
    }
}