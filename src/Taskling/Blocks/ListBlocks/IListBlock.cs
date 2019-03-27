using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Taskling.Blocks.ListBlocks
{
    public interface IListBlock<TItem>
    {
        string ListBlockId { get; }
        int Attempt { get; }
        Task<IList<IListBlockItem<TItem>>> GetItemsAsync();
    }

    public interface IListBlock<TItem, THeader>
    {
        string ListBlockId { get; }
        int Attempt { get; }
        THeader Header { get; }

        Task<IList<IListBlockItem<TItem>>> GetItemsAsync();
    }
}
