using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Taskling.SqlServer.Models;
using Taskling.Tasks;

namespace Taskling.SqlServer.Blocks.QueryBuilders;

public class BlocksOfTaskQueryBuilder
{
    public static readonly int[] PendingAndFailed = { 0, 1, 3 };

    public static Func<BlocksOfTaskQueryParams, Expression<Func<BlocksOfTaskQueryItem, bool>>>
        GetFindDateRangeBlocksOfTaskQuery(ReprocessOption reprocessOption)
    {
        if (reprocessOption == ReprocessOption.Everything)
            return z => i => true;

        if (reprocessOption == ReprocessOption.PendingOrFailed)
            return z => i => PendingAndFailed.Contains(i
                .BlockExecutionStatus);

        throw new ArgumentException("ReprocessOption not supported");
    }

    public static Func<BlocksOfTaskQueryParams, Expression<Func<BlocksOfTaskQueryItem, bool>>>
        GetFindNumericRangeBlocksOfTaskQuery(ReprocessOption reprocessOption)
    {
        if (reprocessOption == ReprocessOption.Everything)
            return z => i => true;


        if (reprocessOption == ReprocessOption.PendingOrFailed)
            return z => i => PendingAndFailed.Contains(i
                .BlockExecutionStatus);

        throw new ArgumentException("ReprocessOption not supported");
    }

    public static Func<BlocksOfTaskQueryParams, Expression<Func<BlocksOfTaskQueryItem, bool>>>
        GetFindListBlocksOfTaskQuery(ReprocessOption reprocessOption)
    {
        if (reprocessOption == ReprocessOption.Everything)
            return z => i => true; // string.Format(GetBlocksOfTaskQuery, "", "");

        if (reprocessOption == ReprocessOption.PendingOrFailed)
            return z => i =>
                new[] { z.NotStarted, z.Started, z.Failed }.Contains(i.BlockExecutionStatus);

        throw new ArgumentException("ReprocessOption not supported");
    }

    public static Func<BlocksOfTaskQueryParams, Expression<Func<BlocksOfTaskQueryItem, bool>>>
        GetFindObjectBlocksOfTaskQuery(ReprocessOption reprocessOption)
    {
        if (reprocessOption == ReprocessOption.Everything)
            return z => i => true; // string.Format(GetBlocksOfTaskQuery, ",B.ObjectData", "");

        if (reprocessOption == ReprocessOption.PendingOrFailed)
            return z => i =>
                new[] { z.NotStarted, z.Started, z.Failed }.Contains(i.BlockExecutionStatus);

        throw new ArgumentException("ReprocessOption not supported");
    }

    public static async Task<List<BlocksOfTaskQueryItem>> GetBlocksOfTaskQueryItems(TasklingDbContext dbContext,
        int taskDefinitionId,
        string referenceValue, Expression<Func<BlocksOfTaskQueryItem, bool>> filterExpression)
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
        var queryable = query.Select(i => new BlocksOfTaskQueryItem
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

    public class BlocksOfTaskQueryItem
    {
        public DateTime CreatedDate { get; set; }
        public long BlockId { get; set; }
        public int Attempt { get; set; }
        public int BlockType { get; set; }
        public string? ObjectData { get; set; }
        public byte[]? CompressedObjectData { get; set; }
        public int TaskDefinitionId { get; set; }
        public string? ReferenceValue { get; set; }
        public int BlockExecutionStatus { get; set; }
        public DateTime? FromDate { get; set; }
        public long? FromNumber { get; set; }
        public DateTime? ToDate { get; set; }
        public long? ToNumber { get; set; }
    }

    public class BlocksOfTaskQueryParams
    {
        public int NotStarted { get; set; }
        public int Started { get; set; }
        public int Failed { get; set; }
    }
}