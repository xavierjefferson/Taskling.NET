﻿using System;
using System.Threading.Tasks;
using Taskling.Blocks.Common;
using Taskling.Contexts;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.Blocks.RangeBlocks;

public class RangeBlockContext : BlockContextBase, IDateRangeBlockContext, INumericRangeBlockContext
{
    private readonly RangeBlock _block;
    private readonly IRangeBlockRepository _rangeBlockRepository;


    public RangeBlockContext(IRangeBlockRepository rangeBlockRepository,
        ITaskExecutionRepository taskExecutionRepository,
        TaskId taskId,
        int taskExecutionId,
        RangeBlock rangeBlock,
        long blockExecutionId,
        int forcedBlockQueueId = 0) : base(taskId, blockExecutionId, taskExecutionId, taskExecutionRepository,
        forcedBlockQueueId)
    {
        _rangeBlockRepository = rangeBlockRepository;
        _block = rangeBlock;
    }


    protected override BlockType BlockType => _block.RangeType;

    protected override Func<BlockExecutionChangeStatusRequest, Task> ChangeStatusFunc =>
        _rangeBlockRepository.ChangeStatusAsync;


    public IDateRangeBlock DateRangeBlock => _block;


    public INumericRangeBlock NumericRangeBlock => _block;


    protected override string GetFailedErrorMessage(string message)
    {
        if (_block.RangeType == BlockType.DateRange)
            return
                $"BlockId {_block.RangeBlockId} From: {_block.RangeBeginAsDateTime().ToString("yyyy-MM-dd HH:mm:ss")} To: {_block.RangeEndAsDateTime().ToString("yyyy-MM-dd HH:mm:ss")} Error: {message}";
        return
            $"BlockId {_block.RangeBlockId} From: {_block.RangeBeginAsLong()} To: {_block.RangeEndAsLong()} Error: {message}";
    }
}