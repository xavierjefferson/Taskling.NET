using Taskling.Blocks.Common;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.SqlServer.Blocks.Models;
using Taskling.SqlServer.Blocks.QueryBuilders;
using Taskling.SqlServer.Blocks.Serialization;

namespace Taskling.SqlServer.Blocks;

public partial class BlockRepository
{
    private static List<RangeBlock> GetRangeBlocks<T>(IBlockRequest request, List<T> items)
        where T : IBlockQueryItem
    {
        var results = new List<RangeBlock>();
        foreach (var item in items)
        {
            var blockType = (BlockType)item.BlockType;
            if (blockType != request.BlockType)
                throw GetBlockTypeException(request, blockType);

            var rangeBlockId = item.BlockId;
            var attempt = item.Attempt;
            long rangeBegin;
            long rangeEnd;
            if (request.BlockType == BlockType.DateRange)
            {
                rangeBegin = item.FromDate.Value.Ticks; //reader.GetDateTime("FromDate").Ticks;
                rangeEnd = item.ToDate.Value.Ticks; //reader.GetDateTime("ToDate").Ticks;
            }
            else
            {
                rangeBegin = item.FromNumber.Value;
                rangeEnd = item.ToNumber.Value;
            }

            results.Add(new RangeBlock(rangeBlockId, attempt, rangeBegin, rangeEnd,
                request.BlockType));
        }

        return results;
    }

    private static List<ObjectBlock<T>> GetObjectBlocks<T, U>(IBlockRequest request, List<U> items)
        where U : IBlockQueryItem
    {
        var results = new List<ObjectBlock<T>>();
        foreach (var item in items)
        {
            var blockType = (BlockType)item.BlockType;
            if (blockType != request.BlockType)
                throw GetBlockTypeException(request, blockType);

            var objectBlock = new ObjectBlock<T>();
            objectBlock.ObjectBlockId = item.BlockId;
            objectBlock.Attempt = item.Attempt;
            objectBlock.Object =
                SerializedValueReader.ReadValue<T>(item.ObjectData, item.CompressedObjectData);

            results.Add(objectBlock);
        }

        return results;
    }

    private static List<ProtoListBlock> GetListBlocks<T>(IBlockRequest blocksOfTaskRequest, List<T> items)
        where T : IBlockQueryItem
    {
        var results = new List<ProtoListBlock>();
        foreach (var reader in items)
        {
            var blockType = (BlockType)reader.BlockType;
            if (blockType != blocksOfTaskRequest.BlockType)
                throw GetBlockTypeException(blocksOfTaskRequest, blockType);


            var listBlock = new ProtoListBlock();
            listBlock.ListBlockId = reader.BlockId;
            listBlock.Attempt = reader.Attempt;
            listBlock.Header =
                SerializedValueReader.ReadValueAsString(reader, i => i.ObjectData,
                    i => i.CompressedObjectData);

            results.Add(listBlock);
        }

        return results;
    }
}