using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks.ListBlocks
{
    public interface IListBlock<TItem>
    {
        string ListBlockId { get; }
        int Attempt { get; }
        IList<IListBlockItem<TItem>> Items { get; }
    }

    public interface IListBlock<TItem, THeader>
    {
        string ListBlockId { get; }
        int Attempt { get; }
        IList<IListBlockItem<TItem>> Items { get; }
        THeader Header { get; }
    }
}
