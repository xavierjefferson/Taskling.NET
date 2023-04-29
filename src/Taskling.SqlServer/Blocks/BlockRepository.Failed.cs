using Taskling.Blocks.Common;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;

namespace Taskling.SqlServer.Blocks;

public partial class BlockRepository
{
    private const string FindFailedBlocksQuery = @"
WITH OrderedBlocks As (
	SELECT ROW_NUMBER() OVER (PARTITION BY BE.BlockId ORDER BY BE.BlockExecutionId DESC) AS RowNo
			,BE.[BlockExecutionId]
	FROM [Taskling].[BlockExecution] BE WITH(NOLOCK)
	JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
	WHERE TE.TaskDefinitionId = @TaskDefinitionId
	AND TE.StartedAt >= @SearchPeriodBegin
    AND TE.StartedAt < @SearchPeriodEnd
)

SELECT TOP {0} B.[BlockId]
    {1}
    ,BE.Attempt
    ,B.BlockType
    ,TE.ReferenceValue
    ,B.ObjectData
    ,B.CompressedObjectData
FROM [Taskling].[Block] B WITH(NOLOCK)
JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId
JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
JOIN OrderedBlocks OB ON BE.BlockExecutionId = OB.BlockExecutionId
WHERE B.TaskDefinitionId = @TaskDefinitionId
AND B.IsPhantom = 0
AND BE.BlockExecutionStatus = 4
AND BE.Attempt < @AttemptLimit
AND OB.RowNo = 1
ORDER BY B.CreatedDate ASC";


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
}