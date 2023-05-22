using Microsoft.EntityFrameworkCore;
using Taskling.Blocks.Common;
using Taskling.EntityFrameworkCore.Blocks.Models;
using Taskling.EntityFrameworkCore.Models;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;

namespace Taskling.EntityFrameworkCore.Blocks.QueryBuilders;

public class DeadBlocksQueryBuilder
{
    private const string FindDeadBlocksQuery = @"WITH OrderedBlocks As (
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
    ,B.ObjectData
    ,B.CompressedObjectData
FROM [Taskling].[Block] B WITH(NOLOCK)
JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId
JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
JOIN OrderedBlocks OB ON BE.BlockExecutionId = OB.BlockExecutionId
WHERE B.TaskDefinitionId = @TaskDefinitionId
AND B.IsPhantom = 0
AND TE.StartedAt <= DATEADD(SECOND, -1 * DATEDIFF(SECOND, '00:00:00', OverrideThreshold), GETUTCDATE())
AND BE.BlockExecutionStatus IN (1,2)
AND BE.Attempt < @AttemptLimit
AND OB.RowNo = 1
ORDER BY B.CreatedDate ASC";

    private const string FindDeadBlocksWithKeepAliveQuery = @"WITH OrderedBlocks As (
	SELECT ROW_NUMBER() OVER (PARTITION BY BE.BlockId ORDER BY BE.BlockExecutionId DESC) AS RowNo
			,BE.[BlockExecutionId]
	FROM [Taskling].[BlockExecution] BE WITH(NOLOCK)
	JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
	WHERE TE.TaskDefinitionId = @TaskDefinitionId
	AND TE.StartedAt  >= @SearchPeriodBegin
    AND TE.StartedAt < @SearchPeriodEnd
)

SELECT TOP {0} B.[BlockId]
    {1}
    ,BE.Attempt
    ,B.BlockType
    ,B.ObjectData
    ,B.CompressedObjectData
FROM [Taskling].[Block] B WITH(NOLOCK)
JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId
JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
JOIN OrderedBlocks OB ON BE.BlockExecutionId = OB.BlockExecutionId
WHERE B.TaskDefinitionId = @TaskDefinitionId
AND B.IsPhantom = 0
AND DATEDIFF(SECOND, TE.LastKeepAlive, GETUTCDATE()) > DATEDIFF(SECOND, '00:00:00', TE.KeepAliveDeathThreshold)
AND BE.BlockExecutionStatus IN (1,2)
AND BE.Attempt < @AttemptLimit
AND OB.RowNo = 1
ORDER BY B.CreatedDate ASC";


    public static async Task<List<BlockQueryItem>> GetFindDeadBlocksQuery(BlockItemRequestWrapper requestWrapper)
    {
        var items = await GetBlocksInner(requestWrapper);
        //AND TE.StartedAt <= DATEADD(SECOND, -1 * DATEDIFF(SECOND, '00:00:00', OverrideThreshold), GETUTCDATE())
        return items.Where(i => i.StartedAt < DateTime.UtcNow.Subtract(i.OverrideThreshold.Value))
            .Take(requestWrapper.Limit).ToList();
    }

    public static async Task<List<BlockQueryItem>> GetFindDeadBlocksWithKeepAliveQuery(
        BlockItemRequestWrapper requestWrapper)
    {
        var items = await GetBlocksInner(requestWrapper);
        //AND DATEDIFF(SECOND, TE.LastKeepAlive, GETUTCDATE()) > DATEDIFF(SECOND, '00:00:00', TE.KeepAliveDeathThreshold)
        return items.Where(i => DateTime.UtcNow.Subtract(i.LastKeepAlive) > i.KeepAliveDeathThreshold)
            .Take(requestWrapper.Limit).ToList();
    }

    public static async Task<List<BlockQueryItem>> GetBlocksInner(BlockItemRequestWrapper requestWrapper)
    {
        var tasklingDbContext = requestWrapper.DbContext;
        var request = requestWrapper.Body;

        var b0 = tasklingDbContext.BlockExecutions.Include(i => i.TaskExecution).Where(i =>
                i.TaskExecution.StartedAt >= request.SearchPeriodBegin
                && i.TaskExecution.StartedAt <= request.SearchPeriodEnd &&
                i.TaskExecution.TaskDefinitionId == requestWrapper.TaskDefinitionId)
            .GroupBy(i => i.BlockId,
                (i, j) => j.Max(k => k.BlockExecutionId));
        var d = b0.ToList();

        var statuses = request.GetMatchingStatuses();

        var b = tasklingDbContext.BlockExecutions.Include(i => i.TaskExecution)
            .Join(b0, i => i.BlockExecutionId, i => i, (i, j) => i)
            .Where(i => !i.Block.IsPhantom && statuses.Contains(i.BlockExecutionStatus) &&
                        i.Attempt < request.AttemptLimit)
            //.Where(i=>i.StartedAt.Value.Add(i.TaskExecution.OverrideThreshold.Value) <= DateTime.UtcNow)
            .OrderBy(i => i.Block.CreatedDate)
            .Select(i => new BlockQueryItem
            {
                BlockId = i.BlockId,
                Attempt = i.Attempt,
                BlockType = i.Block.BlockType,
                FromDate = i.Block.FromDate,
                ObjectData = i.Block.ObjectData,
                CompressedObjectData = i.Block.CompressedObjectData,
                FromNumber = i.Block.FromNumber,
                ToDate = i.Block.ToDate,
                ToNumber = i.Block.ToNumber,
                StartedAt = i.TaskExecution.StartedAt,
                LastKeepAlive = i.TaskExecution.LastKeepAlive,
                OverrideThreshold = i.TaskExecution.OverrideThreshold,
                KeepAliveDeathThreshold = i.TaskExecution.KeepAliveDeathThreshold,
                KeepAliveInterval = i.TaskExecution.KeepAliveInterval
            });


        var c = await b.ToListAsync().ConfigureAwait(false);
        return c;
    }
}

public class BlockItemRequestWrapper
{
    public TasklingDbContext? DbContext { get; set; }
    public int Limit { get; set; }
    public ISearchableBlockRequest? Body { get; set; }
    public BlockType BlockType { get; set; }
    public long TaskDefinitionId { get; set; }
}