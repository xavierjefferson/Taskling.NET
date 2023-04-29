using Taskling.SqlServer.Blocks.Models;

namespace Taskling.SqlServer.Blocks.QueryBuilders;

public class FailedBlocksQueryBuilder
{
    public static async Task<List<BlockQueryItem>> GetFindFailedBlocksQuery(BlockItemRequestWrapper requestWrapper)
    {
        var items = await DeadBlocksQueryBuilder.GetBlocksInner(requestWrapper);
        //AND TE.StartedAt <= DATEADD(SECOND, -1 * DATEDIFF(SECOND, '00:00:00', OverrideThreshold), GETUTCDATE())
        return items.Take(requestWrapper.Limit).ToList(); //.Where(i => i.StartedAt < DateTime.UtcNow.Subtract(i.OverrideThreshold.Value)).Take(limit).ToList();
    }


}