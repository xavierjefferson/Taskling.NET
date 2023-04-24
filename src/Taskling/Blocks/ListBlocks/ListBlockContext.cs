using System;
using Taskling.Contexts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.Blocks.ListBlocks;

public class ListBlockContext<T> : ListBlockContextBase<T, bool>, IListBlockContext<T>, IDisposable
{
    #region .: Constructor :.

    public ListBlockContext(IListBlockRepository listBlockRepository,
        ITaskExecutionRepository taskExecutionRepository,
        string applicationName,
        string taskName,
        int taskExecutionId,
        ListUpdateMode listUpdateMode,
        int uncommittedThreshold,
        ListBlock<T> listBlock,
        long blockExecutionId,
        int maxStatusReasonLength,
        int forcedBlockQueueId = 0)
        : base(listBlockRepository,
            taskExecutionRepository,
            applicationName,
            taskName,
            taskExecutionId,
            listUpdateMode,
            uncommittedThreshold,
            listBlock,
            blockExecutionId,
            maxStatusReasonLength,
            forcedBlockQueueId)
    {
        _headerlessBlock.SetParentContext(this);
    }

    #endregion .: Constructor :.

    public IListBlock<T> Block => _headerlessBlock;
}