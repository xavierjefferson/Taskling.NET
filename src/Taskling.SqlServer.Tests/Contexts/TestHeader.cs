using System;

namespace Taskling.SqlServer.Tests.Contexts;

public class TestHeader
{
    public string PurchaseCode { get; set; }
    public DateTime FromDate { get; set; }
    public DateTime ToDate { get; set; }
}