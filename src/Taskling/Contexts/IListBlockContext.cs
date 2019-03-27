using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Blocks.ListBlocks;

namespace Taskling.Contexts
{
    public interface IListBlockContext<T> : IBlockContext
    {
        IListBlock<T> Block { get; }
        string ListBlockId { get; }
        string ForcedBlockQueueId { get; }
        Task<IEnumerable<IListBlockItem<T>>> GetItemsAsync(params ItemStatus[] statuses);
        Task ItemCompleteAsync(IListBlockItem<T> item);
        Task ItemFailedAsync(IListBlockItem<T> item, string reason, byte? step = null);
        Task DiscardItemAsync(IListBlockItem<T> item, string reason, byte? step = null);
        Task<IEnumerable<T>> GetItemValuesAsync(params ItemStatus[] statuses);
        Task FlushAsync();
    }

    public interface IListBlockContext<TItem, THeader> : IBlockContext
    {
        IListBlock<TItem, THeader> Block { get; }
        string ListBlockId { get; }
        string ForcedBlockQueueId { get; }

        Task<IEnumerable<IListBlockItem<TItem>>> GetItemsAsync(params ItemStatus[] statuses);
        Task ItemCompleteAsync(IListBlockItem<TItem> item);
        Task ItemFailedAsync(IListBlockItem<TItem> item, string reason, byte? step = null);
        Task DiscardItemAsync(IListBlockItem<TItem> item, string reason, byte? step = null);
        Task<IEnumerable<TItem>> GetItemValuesAsync(params ItemStatus[] statuses);
        Task FlushAsync();
    }
}
