using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Taskling.Blocks.Common;
using Taskling.SqlServer.Blocks.Models;
using Taskling.SqlServer.Models;
using Taskling.Tasks;

namespace Taskling.SqlServer.Blocks.QueryBuilders;

public class BlocksOfTaskQueryBuilder
{
    public static readonly int[] Statuses013 =
    {
        (int)BlockExecutionStatus.NotStarted,
        (int)BlockExecutionStatus.NotDefined,
        (int)BlockExecutionStatus.Completed
    };

    public static Func<BlocksOfTaskQueryParams, Expression<Func<BlockQueryItem, bool>>>
        GetFindDateRangeBlocksOfTaskQuery(ReprocessOption reprocessOption)
    {
        if (reprocessOption == ReprocessOption.Everything)
            return z => i => true;

        if (reprocessOption == ReprocessOption.PendingOrFailed)
            return z => i => Statuses013.Contains(i
                .BlockExecutionStatus);

        throw new ArgumentException("ReprocessOption not supported");
    }

    public static Func<BlocksOfTaskQueryParams, Expression<Func<BlockQueryItem, bool>>>
        GetFindNumericRangeBlocksOfTaskQuery(ReprocessOption reprocessOption)
    {
        if (reprocessOption == ReprocessOption.Everything)
            return z => i => true;


        if (reprocessOption == ReprocessOption.PendingOrFailed)
            return z => i => Statuses013.Contains(i
                .BlockExecutionStatus);

        throw new ArgumentException("ReprocessOption not supported");
    }

    public static Func<BlocksOfTaskQueryParams, Expression<Func<BlockQueryItem, bool>>>
        GetFindListBlocksOfTaskQuery(ReprocessOption reprocessOption)
    {
        return GetFindNumericRangeBlocksOfTaskQuery(reprocessOption);
    }

    public static Func<BlocksOfTaskQueryParams, Expression<Func<BlockQueryItem, bool>>>
        GetFindObjectBlocksOfTaskQuery(ReprocessOption reprocessOption)
    {
        if (reprocessOption == ReprocessOption.Everything)
            return z => i => true; // string.Format(GetBlocksOfTaskQuery, ",B.ObjectData", "");

        if (reprocessOption == ReprocessOption.PendingOrFailed)
            return z => i => z.StatusesToMatch.Contains(i.BlockExecutionStatus);

        throw new ArgumentException("ReprocessOption not supported");
    }

    public static async Task<List<BlockQueryItem>> GetBlocksOfTaskQueryItems(TasklingDbContext dbContext,
        int taskDefinitionId,
        string referenceValue, Expression<Func<BlockQueryItem, bool>> filterExpression)
    {
        var leftSide1 = dbContext.BlockExecutions.Include(i => i.Block);
        var query =
            from leftSide in leftSide1
            join subRightSide in dbContext.TaskExecutions on leftSide.BlockExecutionId equals subRightSide
                .TaskExecutionId into gj
            from rightSide in gj.DefaultIfEmpty()
            select new
            {
                leftSide,
                leftSide.Block,
                rightSide
            };
        var queryable = query.Select(i => new BlockQueryItem
            {
                FromDate = i.Block.FromDate,
                FromNumber = i.Block.FromNumber,
                ToDate = i.Block.ToDate,
                ToNumber = i.Block.ToNumber,
                CreatedDate = i.Block.CreatedDate,
                BlockId = i.leftSide.BlockId,
                Attempt = i.leftSide.Attempt,
                BlockExecutionStatus = i.leftSide.BlockExecutionStatus,
                BlockType = i.Block.BlockType,
                TaskDefinitionId = i.Block.TaskDefinitionId,
                ReferenceValue = i.rightSide.ReferenceValue,
                ObjectData = i.Block.ObjectData,
                CompressedObjectData = i.Block.CompressedObjectData
            }).Where(i => i.ReferenceValue == referenceValue && i.TaskDefinitionId == taskDefinitionId)
            .Where(filterExpression);

        return await queryable.OrderBy(i => i.CreatedDate).ToListAsync();
    }
}