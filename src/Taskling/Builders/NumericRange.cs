namespace Taskling.Builders;

public class NumericRange
{
    public NumericRange(int fromNumber, int toNumber, int maxBlockSize)
    {
        FromNumber = fromNumber;
        ToNumber = toNumber;
        MaxBlockSize = maxBlockSize;
    }

    public int FromNumber { get; set; }
    public int ToNumber { get; set; }
    public int MaxBlockSize { get; set; }
}