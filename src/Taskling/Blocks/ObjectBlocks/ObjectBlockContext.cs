using System;
using System.Threading.Tasks;
using Taskling.Blocks.Common;
using Taskling.Blocks.RangeBlocks;
using Taskling.Contexts;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.Blocks.ObjectBlocks;

public class ObjectBlockContext<T> : BlockContextBase, IObjectBlockContext<T>
{
    private readonly IObjectBlockRepository _objectBlockRepository;


    public ObjectBlockContext(IObjectBlockRepository objectBlockRepository,
        ITaskExecutionRepository taskExecutionRepository,
        TaskId taskId,
        int taskExecutionId,
        ObjectBlock<T> block,
        long blockExecutionId,
        int forcedBlockQueueId = 0) : base(taskId, blockExecutionId, taskExecutionId, taskExecutionRepository,
        forcedBlockQueueId)
    {
        _objectBlockRepository = objectBlockRepository;
        Block = block;
    }


    protected override Func<BlockExecutionChangeStatusRequest, Task> ChangeStatusFunc =>
        _objectBlockRepository.ChangeStatusAsync;


    protected override BlockType BlockType => BlockType.Object;


    public IObjectBlock<T> Block { get; }


    protected override string GetFailedErrorMessage(string message)
    {
        return $"BlockId {Block.ObjectBlockId} Error: {message}";
    }


   
}