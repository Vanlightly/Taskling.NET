using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks.ListBlocks
{
    public class ListBlockItem<T> : IListBlockItem<T>
    {
        private ItemCompleteDelegate<T> _itemComplete;
        private ItemFailedDelegate<T> _itemFailed;
        private DiscardItemDelegate<T> _discardItem;

        internal void SetParentContext(ItemCompleteDelegate<T> itemComplete,
            ItemFailedDelegate<T> itemFailed,
            DiscardItemDelegate<T> discardItem)
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

        public void Completed()
        {
            _itemComplete(this);
        }
        public void Failed(string message)
        {
            _itemFailed(this, message);
        }

        public void Discarded(string message)
        {
            _discardItem(this, message);
        }
    }
}
