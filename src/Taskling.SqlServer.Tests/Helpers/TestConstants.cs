using System;
using Taskling.SqlServer.Tests.Enums;

namespace Taskling.SqlServer.Tests.Helpers;

internal class TestConstants
{
    public const string CollectionName = "DefaultCollection";
    internal static TimeSpan QueryTimeout = new(0, 1, 0);
    public static readonly ConnectionTypeEnum ConnectionType = ConnectionTypeEnum.MySql;

    public static string GetTestConnectionString()
    {
        return ConnectionType == ConnectionTypeEnum.SqlServer
            ? "Server=(local);Database=TasklingDb;Application Name=Entity Tester;Trusted_Connection=True;"
            : "Server=localhost;Database=taskling;uid=root;";
    }
}