using System;

namespace Taskling.Builders;

public class DateRange
{
    public DateRange(DateTime fromDate, DateTime toDate, TimeSpan maxBlockSize)
    {
        FromDate = fromDate;
        ToDate = toDate;
        MaxBlockSize = maxBlockSize;
    }

    public DateTime FromDate { get; set; }
    public DateTime ToDate { get; set; }
    public TimeSpan MaxBlockSize { get; set; }
}