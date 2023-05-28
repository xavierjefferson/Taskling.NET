using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.ListBlocks;
using Taskling.Enums;
using Taskling.InfrastructureContracts;

namespace Taskling.EntityFrameworkCore.Tests.Helpers;

public interface IBlocksHelper
{
    int GetListBlockItemCountByStatus(long blockId, ItemStatusEnum status, TaskId taskId);
    long GetLastBlockId(TaskId taskId);
    List<ListBlockItem<T>> GetListBlockItems<T>(long blockId, ItemStatusEnum status, ILoggerFactory loggerFactory,
        TaskId taskId);
    void EnqueueForcedBlock(long blockId, TaskId taskId);
    void InsertPhantomDateRangeBlock(TaskId taskId, DateTime fromDate, DateTime toDate);
    void InsertPhantomNumericBlock(TaskId taskId, long fromId, long toId);
    void InsertPhantomListBlock(TaskId taskId);
    void InsertPhantomObjectBlock(TaskId taskId);
    int GetBlockCount(TaskId taskId);
    long InsertDateRangeBlock(long taskDefinitionId, DateTime fromDate, DateTime toDate, TaskId taskId);
    long InsertDateRangeBlock(long taskDefinitionId, DateTime fromDate, DateTime toDate, DateTime createdAt,
        TaskId taskId);
    long InsertNumericRangeBlock(long taskDefinitionId, long fromNumber, long toNumber, DateTime createdDate,
        TaskId taskId);
    long InsertListBlock(long taskDefinitionId, DateTime createdDate, TaskId taskId, string objectData = null);
    long InsertObjectBlock(long taskDefinitionId, DateTime createdDate, string objectData, TaskId taskId);

    long InsertBlockExecution(long taskExecutionId, long blockId, DateTime createdAt, DateTime? startedAt,
        DateTime? completedAt, BlockExecutionStatusEnum executionStatus, TaskId taskId, int attempt = 1);

    void DeleteBlocks(TaskId taskId);

    int GetBlockExecutionCountByStatus(TaskId taskId,
        BlockExecutionStatusEnum blockExecutionStatus);

    int GetBlockExecutionItemCount(long blockExecutionId, TaskId taskId);
}