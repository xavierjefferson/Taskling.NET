using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.SqlServer.Blocks.Models;
using Taskling.SqlServer.Blocks.QueryBuilders;
using Taskling.SqlServer.Models;

namespace Taskling.SqlServer.Blocks;

public class BlockItemDelegateRunner
{
    private readonly BlockType _blockType;
    private readonly GetBlockItemsDelegate _getBlockItemsDelegate;

    public BlockItemDelegateRunner(int limit, GetBlockItemsDelegate getBlockItemsDelegate, BlockType blockType)
    {
        _getBlockItemsDelegate = getBlockItemsDelegate;
        _blockType = blockType;
        Limit = limit;
    }

    public int Limit { get; }

    public async Task<List<BlockQueryItem>> Execute(TasklingDbContext dbContext,
        ISearchableBlockRequest request, int taskDefinitionId)
    {
        return await _getBlockItemsDelegate(new BlockItemRequestWrapper()
        {
            TaskDefinitionId = taskDefinitionId,
            Limit = Limit,
            Body = request,
            BlockType = _blockType,
            DbContext = dbContext
        });
    }
}