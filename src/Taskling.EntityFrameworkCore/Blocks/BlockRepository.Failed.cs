using Microsoft.EntityFrameworkCore;
using Taskling.Blocks.Common;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.EntityFrameworkCore.Blocks.Models;
using Taskling.EntityFrameworkCore.Models;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.Tasks;

namespace Taskling.EntityFrameworkCore.Blocks;

public partial class BlockRepository
{
    public async Task<IList<ObjectBlock<T>>> FindFailedObjectBlocksAsync<T>(FindFailedBlocksRequest failedBlocksRequest)
    {
        var funcRunner = GetFailedBlockFuncRunner(failedBlocksRequest, BlockType.Object);
        return await FindSearchableObjectBlocksAsync<T>(failedBlocksRequest, funcRunner).ConfigureAwait(false);
    }

    public async Task<IList<ProtoListBlock>> FindFailedListBlocksAsync(FindFailedBlocksRequest failedBlocksRequest)
    {
        var funcRunner = GetFailedBlockFuncRunner(failedBlocksRequest, BlockType.List);
        return await FindSearchableListBlocksAsync(failedBlocksRequest, funcRunner).ConfigureAwait(false);
    }


    public async Task<IList<RangeBlock>> FindFailedRangeBlocksAsync(FindFailedBlocksRequest failedBlocksRequest)
    {
        var funcRunner = GetFailedBlockFuncRunner(failedBlocksRequest, failedBlocksRequest.BlockType);
        return await FindSearchableDateRangeBlocksAsync(failedBlocksRequest, funcRunner).ConfigureAwait(false);
    }

    public static async Task<List<BlockQueryItem>> GetBlocksOfTaskQueryItems(TasklingDbContext dbContext,
        long taskDefinitionId,
        Guid referenceValue, ReprocessOption reprocessOption)
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
        switch (reprocessOption)
        {
            case ReprocessOption.Everything:
                break;
            case ReprocessOption.PendingOrFailed:
                blockQueryItems = blockQueryItems.Where(i => PendingOrFailedStatuses.Contains(i.BlockExecutionStatus));
                break;
            default:
                throw new InvalidOperationException($"Unknown {nameof(ReprocessOption)} value {reprocessOption}");
        }


        return await blockQueryItems.ToListAsync();
    }
}