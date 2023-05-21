namespace Taskling.Blocks.ObjectBlocks;

public interface IObjectBlock<T>
{
    long ObjectBlockId { get; }
    int Attempt { get; set; }
    T? Object { get; }
}