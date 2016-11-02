using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.ListBlocks;

namespace Taskling.Contexts
{
    public interface IListBlockContext<T> : IBlockContext
    {
        IListBlock<T> Block { get; }
        string ListBlockId { get; }
        string ForcedBlockQueueId { get; }
        IEnumerable<IListBlockItem<T>> GetItems(params ItemStatus[] statuses);
        void ItemComplete(IListBlockItem<T> item);
        void ItemFailed(IListBlockItem<T> item, string reason, byte? step = null);
        void DiscardItem(IListBlockItem<T> item, string reason, byte? step = null);
        IEnumerable<T> GetItemValues(params ItemStatus[] statuses);
    }

    public interface IListBlockContext<TItem, THeader> : IBlockContext
    {
        IListBlock<TItem, THeader> Block { get; }
        string ListBlockId { get; }
        string ForcedBlockQueueId { get; }
        IEnumerable<IListBlockItem<TItem>> GetItems(params ItemStatus[] statuses);
        void ItemComplete(IListBlockItem<TItem> item);
        void ItemFailed(IListBlockItem<TItem> item, string reason, byte? step = null);
        void DiscardItem(IListBlockItem<TItem> item, string reason, byte? step = null);
        IEnumerable<TItem> GetItemValues(params ItemStatus[] statuses);
    }
}
