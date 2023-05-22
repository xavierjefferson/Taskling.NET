using Taskling.EntityFrameworkCore.Blocks.QueryBuilders;

namespace Taskling.EntityFrameworkCore.Blocks.Models;

public delegate Task<List<BlockQueryItem>> GetBlockItemsDelegate(BlockItemRequestWrapper requestWrapper);