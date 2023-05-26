using Microsoft.EntityFrameworkCore;
using Taskling.EntityFrameworkCore.Blocks.Models;
using Taskling.EntityFrameworkCore.Models;
using Taskling.Enums;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;

namespace Taskling.EntityFrameworkCore.Blocks.QueryBuilders;

public class BlockItemRequestWrapper
{
    public TasklingDbContext? DbContext { get; set; }
    public int Limit { get; set; }
    public ISearchableBlockRequest? Body { get; set; }
    public BlockTypeEnum BlockType { get; set; }
    public long TaskDefinitionId { get; set; }
}