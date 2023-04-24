using Taskling.Blocks.ObjectBlocks;

namespace Taskling.InfrastructureContracts.Blocks.ObjectBlocks;

public class ObjectBlockCreateResponse<T>
{
    public ObjectBlock<T> Block { get; set; }
}