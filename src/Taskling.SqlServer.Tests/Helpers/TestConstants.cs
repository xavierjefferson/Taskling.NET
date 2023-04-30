using System;

namespace Taskling.SqlServer.Tests.Helpers;

internal class TestConstants
{
    internal const string TestConnectionString =
        "Server=(local);Database=TasklingDb;Application Name=Entity Tester;Trusted_Connection=True;";

    internal const string ApplicationName = "MyTestApplication";
    internal const string TaskName = "MyTestTask";
    internal static TimeSpan QueryTimeout = new(0, 1, 0);
}