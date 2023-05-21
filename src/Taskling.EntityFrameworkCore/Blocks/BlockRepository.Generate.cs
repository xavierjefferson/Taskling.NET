using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.SqlServer.Blocks.Models;
using Taskling.SqlServer.Blocks.Serialization;

namespace Taskling.SqlServer.Blocks;

public partial class BlockRepository
{
    private List<RangeBlock> GetRangeBlocks<T>(IBlockRequest request, List<T> blockQueryItems)
        where T : IBlockQueryItem
    {
        var logger = _loggerFactory.CreateLogger<RangeBlock>();
        var results = new List<RangeBlock>();
        foreach (var blockQueryItem in blockQueryItems)
        {
            var blockType = (BlockType)blockQueryItem.BlockType;
            if (blockType != request.BlockType)
                throw GetBlockTypeException(request, blockType);

            long rangeBegin;
            long rangeEnd;
            if (request.BlockType == BlockType.DateRange)
            {
                rangeBegin = blockQueryItem.FromDate.Value.Ticks; //reader.GetDateTime("FromDate").Ticks;
                rangeEnd = blockQueryItem.ToDate.Value.Ticks; //reader.GetDateTime("ToDate").Ticks;
            }
            else
            {
                rangeBegin = blockQueryItem.FromNumber.Value;
                rangeEnd = blockQueryItem.ToNumber.Value;
            }


            results.Add(new RangeBlock(blockQueryItem.BlockId, blockQueryItem.Attempt, rangeBegin, rangeEnd,
                request.BlockType, logger));
        }

        return results;
    }

    private static List<ObjectBlock<T>> GetObjectBlocks<T, U>(IBlockRequest request, List<U> blockQueryItems)
        where U : IBlockQueryItem
    {
        var results = new List<ObjectBlock<T>>();
        foreach (var blockQueryItem in blockQueryItems)
        {
            var blockType = (BlockType)blockQueryItem.BlockType;
            if (blockType != request.BlockType)
                throw GetBlockTypeException(request, blockType);

            var objectBlock = new ObjectBlock<T>();
            objectBlock.ObjectBlockId = blockQueryItem.BlockId;
            objectBlock.Attempt = blockQueryItem.Attempt;
            objectBlock.Object =
                SerializedValueReader.ReadValue<T>(blockQueryItem.ObjectData, blockQueryItem.CompressedObjectData);

            results.Add(objectBlock);
        }

        return results;
    }

    private static List<ProtoListBlock> GetListBlocks<T>(IBlockRequest blocksOfTaskRequest, List<T> blockQueryItems)
        where T : IBlockQueryItem
    {
        var results = new List<ProtoListBlock>();
        foreach (var blockQueryItem in blockQueryItems)
        {
            var blockType = (BlockType)blockQueryItem.BlockType;
            if (blockType != blocksOfTaskRequest.BlockType)
                throw GetBlockTypeException(blocksOfTaskRequest, blockType);


            var listBlock = new ProtoListBlock();
            listBlock.ListBlockId = blockQueryItem.BlockId;
            listBlock.Attempt = blockQueryItem.Attempt;
            listBlock.Header =
                SerializedValueReader.ReadValueAsString(blockQueryItem.ObjectData, blockQueryItem.CompressedObjectData);

            results.Add(listBlock);
        }

        return results;
    }
}