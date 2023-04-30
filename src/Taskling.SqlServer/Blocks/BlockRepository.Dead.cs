using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using Taskling.Blocks.Common;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Blocks.QueryBuilders;
using Taskling.Tasks;

namespace Taskling.SqlServer.Blocks;

public partial class BlockRepository
{
    public async Task<IList<ProtoListBlock>> FindDeadListBlocksAsync(FindDeadBlocksRequest deadBlocksRequest)
    {
        var funcRunner = GetDeadBlockFuncRunner(deadBlocksRequest, BlockType.List);
        return await FindSearchableListBlocksAsync(deadBlocksRequest, funcRunner).ConfigureAwait(false);
    }

    public async Task<IList<RangeBlock>> FindDeadRangeBlocksAsync(FindDeadBlocksRequest deadBlocksRequest)
    {
        var funcRunner = GetDeadBlockFuncRunner(deadBlocksRequest, deadBlocksRequest.BlockType);
        return await FindSearchableDateRangeBlocksAsync(deadBlocksRequest, funcRunner).ConfigureAwait(false);
    }

    public async Task<IList<ObjectBlock<T>>> FindDeadObjectBlocksAsync<T>(FindDeadBlocksRequest deadBlocksRequest)
    {
        var funcRunner = GetDeadBlockFuncRunner(deadBlocksRequest, BlockType.Object);
        return await FindSearchableObjectBlocksAsync<T>(deadBlocksRequest, funcRunner).ConfigureAwait(false);
    }

    private static BlockItemDelegateRunner GetFailedBlockFuncRunner(FindFailedBlocksRequest failedBlocksRequest, BlockType blockType)
    {
        BlockItemDelegateRunner query;
        ;
        if (failedBlocksRequest.BlockType == blockType)
            query = new BlockItemDelegateRunner(failedBlocksRequest
                .BlockCountLimit, FailedBlocksQueryBuilder.GetFindFailedBlocksQuery, blockType);
        else
            throw new NotSupportedException(UnexpectedBlockTypeMessage);

        return query;
    }

    private static BlockItemDelegateRunner GetDeadBlockFuncRunner(FindDeadBlocksRequest deadBlocksRequest, BlockType blockType)
    {
        BlockItemDelegateRunner query;
        ;
        if (deadBlocksRequest.BlockType == blockType)
        {
            if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                query = new BlockItemDelegateRunner(deadBlocksRequest
                    .BlockCountLimit, DeadBlocksQueryBuilder.GetFindDeadBlocksWithKeepAliveQuery, blockType);
            else
                query = new BlockItemDelegateRunner(deadBlocksRequest
                    .BlockCountLimit, DeadBlocksQueryBuilder.GetFindDeadBlocksQuery, blockType);
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

        return await RetryHelper.WithRetry(async () =>
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
        return await RetryHelper.WithRetry(async () =>
        {
       
            var taskDefinition =
                await _taskRepository.EnsureTaskDefinitionAsync(deadBlocksRequest.TaskId).ConfigureAwait(false);
            using (var dbContext = await GetDbContextAsync(deadBlocksRequest.TaskId).ConfigureAwait(false))
            {
                var items = await GetBlockQueryItems(deadBlocksRequest, blockItemDelegateRunner, taskDefinition, dbContext);
                var results = GetListBlocks(deadBlocksRequest, items);
                _logger.LogDebug($"{nameof(FindSearchableListBlocksAsync)} is returning {results.Count} rows");
                return results;
            }

        });
    }
}