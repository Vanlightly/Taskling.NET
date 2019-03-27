using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Taskling.Blocks.ListBlocks
{
    public class ListBlockItem<T> : IListBlockItem<T>
    {
        private Func<IListBlockItem<T>, Task> _itemComplete;
        private Func<IListBlockItem<T>,string, byte?, Task> _itemFailed;
        private Func<IListBlockItem<T>, string, byte?, Task> _discardItem;

        internal void SetParentContext(Func<IListBlockItem<T>, Task> itemComplete,
            Func<IListBlockItem<T>, string, byte?, Task> itemFailed,
            Func<IListBlockItem<T>, string, byte?, Task> discardItem)
        {
            _itemComplete = itemComplete;
            _itemFailed = itemFailed;
            _discardItem = discardItem;
        }

        public string ListBlockItemId { get; set; }
        public T Value { get; set; }
        public ItemStatus Status { get; set; }
        public string StatusReason { get; set; }
        public DateTime LastUpdated { get; set; }
        public byte? Step { get; set; }

        public async Task CompletedAsync()
        {
            await _itemComplete(this);
        }
        public async Task FailedAsync(string message)
        {
            await _itemFailed(this, message, null);
        }

        public async Task DiscardedAsync(string message)
        {
            await _discardItem(this, message, null);
        }
    }
}
