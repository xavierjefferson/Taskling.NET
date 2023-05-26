namespace Taskling.Builders;

public class NumericRange
{
    public NumericRange(long fromNumber, long toNumber, long maxBlockSize)
    {
        FromNumber = fromNumber;
        ToNumber = toNumber;
        MaxBlockSize = maxBlockSize;
    }

    public long FromNumber { get; set; }
    public long ToNumber { get; set; }
    public long MaxBlockSize { get; set; }
}