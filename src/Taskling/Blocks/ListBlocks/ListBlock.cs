using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Taskling.Contexts;

namespace Taskling.Blocks.ListBlocks;

 
public class ListBlock<T> : IListBlock<T>
{
    private IListBlockContext<T> _parentContext;

    public ListBlock()
    {
        Items = new List<IListBlockItem<T>>();
    }

    public IList<IListBlockItem<T>> Items { get; set; }

    public long ListBlockId { get; set; }
    public int Attempt { get; set; }

    public async Task<IList<IListBlockItem<T>>> GetItemsAsync()
    {
        if (Items == null || !Items.Any())
            if (_parentContext != null)
                await _parentContext.FillItemsAsync().ConfigureAwait(false);

        return Items;
    }

    internal void SetParentContext(IListBlockContext<T> parentContext)
    {
        _parentContext = parentContext;
    }
}

public class ListBlock<TItem, THeader> : IListBlock<TItem, THeader>
{
    private IListBlockContext<TItem, THeader> _parentContext;

    public ListBlock()
    {
        Items = new List<IListBlockItem<TItem>>();
    }

    internal IList<IListBlockItem<TItem>> Items { get; set; }

    public long ListBlockId { get; set; }
    public int Attempt { get; set; }
    public THeader Header { get; set; }

    public async Task<IList<IListBlockItem<TItem>>> GetItemsAsync()
    {
        if (Items == null) await _parentContext.FillItemsAsync().ConfigureAwait(false);

        return Items;
    }

    internal void SetParentContext(IListBlockContext<TItem, THeader> parentContext)
    {
        _parentContext =  parentContext;
    }
}