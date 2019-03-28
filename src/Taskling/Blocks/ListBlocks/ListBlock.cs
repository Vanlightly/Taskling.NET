using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Contexts;

namespace Taskling.Blocks.ListBlocks
{
    public class ListBlock<T> : IListBlock<T>
    {
        private IList<IListBlockItem<T>> _items;
        private ListBlockContext<T> _parentContext;

        public ListBlock()
        {
            _items = new List<IListBlockItem<T>>();
        }

        internal void SetParentContext(IListBlockContext<T> parentContext)
        {
            _parentContext = (ListBlockContext<T>)parentContext;
        }

        public string ListBlockId { get; set; }
        public int Attempt { get; set; }

        public IList<IListBlockItem<T>> Items
        {
            get { return _items; }
            set { _items = value; }
        }

        public async Task<IList<IListBlockItem<T>>> GetItemsAsync()
        {
            if (_items == null || !_items.Any())
            {
                if (_parentContext != null)
                   await _parentContext.FillItemsAsync().ConfigureAwait(false);
            }

            return _items;
        }
    }

    public class ListBlock<TItem, THeader> : IListBlock<TItem, THeader>
    {
        private IList<IListBlockItem<TItem>> _items;
        private ListBlockContext<TItem, THeader> _parentContext;

        public ListBlock()
        {
            _items = new List<IListBlockItem<TItem>>();
        }

        internal void SetParentContext(IListBlockContext<TItem, THeader> parentContext)
        {
            _parentContext = (ListBlockContext<TItem,THeader>)parentContext;
        }

        public string ListBlockId { get; set; }
        public int Attempt { get; set; }
        public THeader Header { get; set; }

        internal IList<IListBlockItem<TItem>> Items
        {
            get { return _items; }
            set { _items = value; }
        }

        public async Task<IList<IListBlockItem<TItem>>> GetItemsAsync()
        {
            if (_items == null)
            {
                await _parentContext.FillItemsAsync().ConfigureAwait(false);
            }

            return _items;
        }
    }
}
