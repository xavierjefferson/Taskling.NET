using Microsoft.EntityFrameworkCore;
using Taskling.Blocks.Common;
using Taskling.SqlServer.Models;

namespace Taskling.SqlServer.Blocks.QueryBuilders;

public class ForcedBlockQueueQueryBuilder
{
    public static async Task<List<ForcedBlockQueueQueryItem>> GetForcedBlockQueueQueryItems(TasklingDbContext dbContext,
        long taskDefinitionId, BlockType blockType)
    {
        var getData = false;
        switch (blockType)
        {
            case BlockType.List:
            case BlockType.Object:
                getData = true;
                break;
            case BlockType.NumericRange:
            case BlockType.DateRange:
            case BlockType.NotDefined:
                getData = false;
                break;
            default:
                throw new NotImplementedException($"No handling for {nameof(BlockType)} = {blockType}");
        }

        var forcedBlockQueueQueryItems = from leftSide in dbContext.ForceBlockQueues.Include(i => i.Block)
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
                    ForceBlockQueueId = x.leftSide.ForceBlockQueueId,
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
                    ForceBlockQueueId = x.leftSide.ForceBlockQueueId,
                    TaskDefinitionId = x.leftSide.Block.TaskDefinitionId,
                    ProcessingStatus = x.leftSide.ProcessingStatus
                };

        var list = await queryable.Where(i => i.TaskDefinitionId == taskDefinitionId && i.ProcessingStatus == "Pending")
            .ToListAsync();
        return list;
    }

    public class ForcedBlockQueueQueryItem
    {
        public DateTime? FromDate { get; set; }
        public DateTime? ToDate { get; set; }
        public int BlockType { get; set; }
        public string? ProcessingStatus { get; set; }
        public long TaskDefinitionId { get; set; }
        public long BlockId { get; set; }
        public int ForceBlockQueueId { get; set; }
        public long? FromNumber { get; set; }
        public long? ToNumber { get; set; }
        public int Attempt { get; set; }
        public string? ObjectData { get; set; }
        public byte[]? CompressedObjectData { get; set; }
    }
}