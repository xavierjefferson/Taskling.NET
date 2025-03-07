﻿using Taskling.EntityFrameworkCore.Blocks.Models;
using Taskling.EntityFrameworkCore.Blocks.QueryBuilders;
using Taskling.EntityFrameworkCore.Models;
using Taskling.Enums;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;

namespace Taskling.EntityFrameworkCore.Blocks;

public class BlockItemDelegateRunner
{
    private readonly BlockTypeEnum _blockType;
    private readonly GetBlockItemsDelegate _getBlockItemsDelegate;

    public BlockItemDelegateRunner(int limit, GetBlockItemsDelegate getBlockItemsDelegate, BlockTypeEnum blockType)
    {
        _getBlockItemsDelegate = getBlockItemsDelegate;
        _blockType = blockType;
        Limit = limit;
    }

    public int Limit { get; }

    public async Task<List<BlockQueryItem>> Execute(TasklingDbContext dbContext,
        ISearchableBlockRequest request, long taskDefinitionId)
    {
        return await _getBlockItemsDelegate(new BlockItemRequestWrapper
        {
            TaskDefinitionId = taskDefinitionId,
            Limit = Limit,
            Body = request,
            BlockType = _blockType,
            DbContext = dbContext
        });
    }
}