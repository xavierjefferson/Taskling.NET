using Microsoft.Extensions.Logging;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.EntityFrameworkCore.Blocks.QueryBuilders;
using Taskling.Enums;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;

namespace Taskling.EntityFrameworkCore.Blocks;

public partial class BlockRepository
{
    public async Task<IList<ProtoListBlock>> FindDeadListBlocksAsync(FindDeadBlocksRequest deadBlocksRequest)
    {
        var funcRunner = GetDeadBlockFuncRunner(deadBlocksRequest, BlockTypeEnum.List);
        return await FindSearchableListBlocksAsync(deadBlocksRequest, funcRunner).ConfigureAwait(false);
    }

    public async Task<IList<RangeBlock>> FindDeadRangeBlocksAsync(FindDeadBlocksRequest deadBlocksRequest)
    {
        var funcRunner = GetDeadBlockFuncRunner(deadBlocksRequest, deadBlocksRequest.BlockType);
        return await FindSearchableDateRangeBlocksAsync(deadBlocksRequest, funcRunner).ConfigureAwait(false);
    }

    public async Task<IList<ObjectBlock<T>>> FindDeadObjectBlocksAsync<T>(FindDeadBlocksRequest deadBlocksRequest)
    {
        var funcRunner = GetDeadBlockFuncRunner(deadBlocksRequest, BlockTypeEnum.Object);
        return await FindSearchableObjectBlocksAsync<T>(deadBlocksRequest, funcRunner).ConfigureAwait(false);
    }

    private static BlockItemDelegateRunner GetFailedBlockFuncRunner(FindFailedBlocksRequest failedBlocksRequest,
        BlockTypeEnum blockType)
    {
        BlockItemDelegateRunner query;
        ;
        if (failedBlocksRequest.BlockType == blockType)
            query = new BlockItemDelegateRunner(failedBlocksRequest
                .BlockCountLimit, GetFailedBlocks, blockType);
        else
            throw new NotSupportedException(UnexpectedBlockTypeMessage);

        return query;
    }

    private static BlockItemDelegateRunner GetDeadBlockFuncRunner(FindDeadBlocksRequest deadBlocksRequest,
        BlockTypeEnum blockType)
    {
        BlockItemDelegateRunner query;
        ;
        if (deadBlocksRequest.BlockType == blockType)
        {
            if (deadBlocksRequest.TaskDeathMode == TaskDeathModeEnum.KeepAlive)
                query = new BlockItemDelegateRunner(deadBlocksRequest
                    .BlockCountLimit, GetDeadBlocksWithKeepAlive, blockType);
            else
                query = new BlockItemDelegateRunner(deadBlocksRequest
                    .BlockCountLimit, GetDeadBlocks, blockType);
        }
        else
        {
            throw new NotSupportedException(UnexpectedBlockTypeMessage);
        }

        return query;
    }

    private async Task<List<RangeBlock>> FindSearchableDateRangeBlocksAsync(ISearchableBlockRequest request,
        BlockItemDelegateRunner blockItemDelegateRunner)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var taskDefinition =
                await _taskRepository.EnsureTaskDefinitionAsync(request.TaskId).ConfigureAwait(false);
            using (var dbContext = await GetDbContextAsync(request.TaskId).ConfigureAwait(false))
            {
                var items = await GetBlockQueryItems(request, blockItemDelegateRunner, taskDefinition, dbContext);

                return GetRangeBlocks(request, items);
            }
        });
    }

    private async Task<IList<ProtoListBlock>> FindSearchableListBlocksAsync(ISearchableBlockRequest deadBlocksRequest,
        BlockItemDelegateRunner blockItemDelegateRunner)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var taskDefinition =
                await _taskRepository.EnsureTaskDefinitionAsync(deadBlocksRequest.TaskId).ConfigureAwait(false);
            using (var dbContext = await GetDbContextAsync(deadBlocksRequest.TaskId).ConfigureAwait(false))
            {
                var items = await GetBlockQueryItems(deadBlocksRequest, blockItemDelegateRunner, taskDefinition,
                    dbContext);
                var results = GetListBlocks(deadBlocksRequest, items);
                _logger.LogDebug($"{nameof(FindSearchableListBlocksAsync)} is returning {results.Count} rows");
                return results;
            }
        });
    }
}