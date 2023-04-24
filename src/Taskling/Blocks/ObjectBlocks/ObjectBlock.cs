namespace Taskling.Blocks.ObjectBlocks;

public class ObjectBlock<T> : IObjectBlock<T>
{
    public long ObjectBlockId { get; set; }
    public int Attempt { get; set; }
    public T Object { get; set; }
}