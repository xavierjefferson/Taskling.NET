using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.RangeBlocks;
using Taskling.Contexts;
using Taskling.Enums;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Retries;

namespace Taskling.Blocks.ObjectBlocks;

public class ObjectBlockContext<T> : BlockContextBase, IObjectBlockContext<T>
{
    private readonly ILogger<ObjectBlockContext<T>> _logger;
    private readonly IObjectBlockRepository _objectBlockRepository;

    public ObjectBlockContext(ILoggerFactory loggerFactory,
        IObjectBlockRepository objectBlockRepository,
        IRetryService retryService,
        ITaskExecutionRepository taskExecutionRepository,
        TaskId taskId,
        long taskExecutionId,
        ObjectBlock<T> block,
        long blockExecutionId,
        long forcedBlockQueueId = 0) : base(taskId, blockExecutionId, taskExecutionId, retryService,
        taskExecutionRepository, loggerFactory.CreateLogger<BlockContextBase>(),
        forcedBlockQueueId)
    {
        _logger = loggerFactory.CreateLogger<ObjectBlockContext<T>>();
        _objectBlockRepository = objectBlockRepository;
        Block = block;
    }

    protected override Func<BlockExecutionChangeStatusRequest, Task> ChangeStatusFunc =>
        _objectBlockRepository.ChangeStatusAsync;

    protected override BlockTypeEnum BlockType => BlockTypeEnum.Object;
    public IObjectBlock<T> Block { get; }

    protected override string GetFailedErrorMessage(string message)
    {
        return $"BlockId {Block.ObjectBlockId} Error: {message}";
    }
}