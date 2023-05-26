using Microsoft.EntityFrameworkCore;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.EntityFrameworkCore.Blocks.Models;
using Taskling.EntityFrameworkCore.Blocks.QueryBuilders;
using Taskling.EntityFrameworkCore.Models;
using Taskling.Enums;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;

namespace Taskling.EntityFrameworkCore.Blocks;

public partial class BlockRepository
{
    public async Task<IList<ObjectBlock<T>>> FindFailedObjectBlocksAsync<T>(FindFailedBlocksRequest failedBlocksRequest)
    {
        var funcRunner = GetFailedBlockFuncRunner(failedBlocksRequest, BlockTypeEnum.Object);
        return await FindSearchableObjectBlocksAsync<T>(failedBlocksRequest, funcRunner).ConfigureAwait(false);
    }

    public async Task<IList<ProtoListBlock>> FindFailedListBlocksAsync(FindFailedBlocksRequest failedBlocksRequest)
    {
        var funcRunner = GetFailedBlockFuncRunner(failedBlocksRequest, BlockTypeEnum.List);
        return await FindSearchableListBlocksAsync(failedBlocksRequest, funcRunner).ConfigureAwait(false);
    }

    public async Task<IList<RangeBlock>> FindFailedRangeBlocksAsync(FindFailedBlocksRequest failedBlocksRequest)
    {
        var funcRunner = GetFailedBlockFuncRunner(failedBlocksRequest, failedBlocksRequest.BlockType);
        return await FindSearchableDateRangeBlocksAsync(failedBlocksRequest, funcRunner).ConfigureAwait(false);
    }

    public static async Task<List<BlockQueryItem>> GetBlocksOfTaskQueryItems(TasklingDbContext dbContext,
        long taskDefinitionId,
        Guid referenceValue, ReprocessOptionEnum reprocessOptionEnum)
    {
        var leftSide1 = dbContext.Blocks.Join(dbContext.BlockExecutions, i => i.BlockId, j => j.BlockId,
            (i, j) => new
            {
                i.FromDate,
                i.FromNumber,
                i.ToDate,
                i.ToNumber,
                i.CreatedDate,
                i.BlockId,
                j.Attempt,
                j.BlockExecutionStatus,
                i.BlockType,
                i.TaskDefinitionId,
                i.ObjectData,
                i.CompressedObjectData,
                j.TaskExecutionId
            });
        var query =
            from leftSide in leftSide1
            join subRightSide in dbContext.TaskExecutions on leftSide.TaskExecutionId equals subRightSide
                .TaskExecutionId into gj
            from rightSide in gj.DefaultIfEmpty()
            select new BlockQueryItem
            {
                FromDate = leftSide.FromDate,
                FromNumber = leftSide.FromNumber,
                ToDate = leftSide.ToDate,
                ToNumber = leftSide.ToNumber,
                CreatedDate = leftSide.CreatedDate,
                BlockId = leftSide.BlockId,
                Attempt = leftSide.Attempt,
                BlockExecutionStatus = leftSide.BlockExecutionStatus,
                BlockType = leftSide.BlockType,
                TaskDefinitionId = leftSide.TaskDefinitionId,
                ReferenceValue = rightSide.ReferenceValue,
                ObjectData = leftSide.ObjectData,
                CompressedObjectData = leftSide.CompressedObjectData
            };
        var blockQueryItems = query.OrderBy(i => i.CreatedDate).Where(i =>
            i.ReferenceValue == referenceValue && i.TaskDefinitionId == taskDefinitionId);
        switch (reprocessOptionEnum)
        {
            case ReprocessOptionEnum.Everything:
                break;
            case ReprocessOptionEnum.PendingOrFailed:
                blockQueryItems = blockQueryItems.Where(i => PendingOrFailedStatuses.Contains(i.BlockExecutionStatus));
                break;
            default:
                throw new InvalidOperationException(
                    $"Unknown {nameof(ReprocessOptionEnum)} value {reprocessOptionEnum}");
        }

        return await blockQueryItems.ToListAsync();
    }

    static async Task<List<BlockQueryItem>> GetDeadBlocksWithKeepAlive(
         BlockItemRequestWrapper requestWrapper)
    {
        var items = await GetBlocksInner(requestWrapper);
        //AND DATEDIFF(SECOND, TE.LastKeepAlive, GETUTCDATE()) > DATEDIFF(SECOND, '00:00:00', TE.KeepAliveDeathThreshold)
        return Enumerable.Where<BlockQueryItem>(items, i => DateTime.UtcNow.Subtract(i.LastKeepAlive) > i.KeepAliveDeathThreshold)
            .Take(requestWrapper.Limit).ToList();
    }

    public static async Task<List<ForcedBlockQueueQueryItem>> GetForcedBlockQueueQueryItems(TasklingDbContext dbContext,
        long taskDefinitionId, BlockTypeEnum blockType)
    {
        var getData = false;
        switch (blockType)
        {
            case BlockTypeEnum.List:
            case BlockTypeEnum.Object:
                getData = true;
                break;
            case BlockTypeEnum.NumericRange:
            case BlockTypeEnum.DateRange:
            case BlockTypeEnum.NotDefined:
                getData = false;
                break;
            default:
                throw new NotImplementedException($"No handling for {nameof(BlockTypeEnum)} = {blockType}");
        }

        var forcedBlockQueueQueryItems = from leftSide in dbContext.ForcedBlockQueues.Include(i => i.Block)
            join preRightSide in dbContext.BlockExecutions.GroupBy(i => i.BlockId)
                    .Select(i => new { BlockId = i.Key, Attempt = i.Max(j => j.Attempt) }) on leftSide.BlockId equals
                preRightSide.BlockId into gj
            from rightSide in gj.DefaultIfEmpty()
            select new { leftSide, rightSide, Attempt = rightSide == null ? 0 : rightSide.Attempt };
        IQueryable<ForcedBlockQueueQueryItem> queryable;
        if (getData)
            queryable =
                from x in forcedBlockQueueQueryItems
                select new ForcedBlockQueueQueryItem
                {
                    BlockId = x.leftSide.BlockId,
                    FromNumber = x.leftSide.Block.FromNumber,
                    FromDate = x.leftSide.Block.FromDate,
                    ToNumber = x.leftSide.Block.ToNumber,
                    ToDate = x.leftSide.Block.ToDate,
                    Attempt = x.rightSide == null ? 0 : x.rightSide.Attempt,
                    BlockType = x.leftSide.Block.BlockType,
                    ForcedBlockQueueId = x.leftSide.ForcedBlockQueueId,
                    ObjectData = x.leftSide.Block.ObjectData,
                    CompressedObjectData = x.leftSide.Block.CompressedObjectData,
                    TaskDefinitionId = x.leftSide.Block.TaskDefinitionId,
                    ProcessingStatus = x.leftSide.ProcessingStatus
                };
        else
            queryable =
                from x in forcedBlockQueueQueryItems
                select new ForcedBlockQueueQueryItem
                {
                    BlockId = x.leftSide.BlockId,
                    FromNumber = x.leftSide.Block.FromNumber,
                    FromDate = x.leftSide.Block.FromDate,
                    ToNumber = x.leftSide.Block.ToNumber,
                    ToDate = x.leftSide.Block.ToDate,
                    Attempt = x.rightSide == null ? 0 : x.rightSide.Attempt,
                    BlockType = x.leftSide.Block.BlockType,
                    ForcedBlockQueueId = x.leftSide.ForcedBlockQueueId,
                    TaskDefinitionId = x.leftSide.Block.TaskDefinitionId,
                    ProcessingStatus = x.leftSide.ProcessingStatus
                };

        var list = await queryable.Where(i => i.TaskDefinitionId == taskDefinitionId && i.ProcessingStatus == "Pending")
            .ToListAsync();
        return list;
    }
}