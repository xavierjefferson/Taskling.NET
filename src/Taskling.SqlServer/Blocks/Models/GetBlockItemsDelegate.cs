using Taskling.SqlServer.Blocks.QueryBuilders;

namespace Taskling.SqlServer.Blocks.Models;

public delegate Task<List<BlockQueryItem>> GetBlockItemsDelegate(BlockItemRequestWrapper requestWrapper);