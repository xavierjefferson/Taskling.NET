using System;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;
using Taskling.Blocks.RangeBlocks;
using Taskling.Contexts;
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
        int taskExecutionId,
        ObjectBlock<T> block,
        long blockExecutionId,
        int forcedBlockQueueId = 0) : base(taskId, blockExecutionId, taskExecutionId, retryService,
        taskExecutionRepository, loggerFactory.CreateLogger<BlockContextBase>(),
        forcedBlockQueueId)
    {
        _logger = loggerFactory.CreateLogger<ObjectBlockContext<T>>();
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _objectBlockRepository = objectBlockRepository;
        Block = block;
    }


    protected override Func<BlockExecutionChangeStatusRequest, Task> ChangeStatusFunc =>
        _objectBlockRepository.ChangeStatusAsync;


    protected override BlockType BlockType => BlockType.Object;


    public IObjectBlock<T> Block { get; }


    protected override string GetFailedErrorMessage(string message)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return $"BlockId {Block.ObjectBlockId} Error: {message}";
    }
}