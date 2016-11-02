using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Contexts;

namespace Taskling.Blocks.ListBlocks
{
    public class ListBlock<T> : IListBlock<T>
    {
        private IList<IListBlockItem<T>> _items;
        private ListBlockContext<T> _parentContext;

        public ListBlock()
        {
            Items = new List<IListBlockItem<T>>();
        }

        internal void SetParentContext(IListBlockContext<T> parentContext)
        {
            _parentContext = (ListBlockContext<T>)parentContext;
        }

        public string ListBlockId { get; set; }
        public int Attempt { get; set; }
        public IList<IListBlockItem<T>> Items
        {
            get
            {
                if (_items == null || !_items.Any())
                {
                    if (_parentContext != null)
                        _parentContext.FillItems();
                }

                return _items;
            }
            set
            {
                _items = value;
            }
        }
    }

    public class ListBlock<TItem, THeader> : IListBlock<TItem, THeader>
    {
        private IList<IListBlockItem<TItem>> _items;
        private IListBlockContext<TItem, THeader> _parentContext;

        public ListBlock()
        {
            Items = new List<IListBlockItem<TItem>>();
        }

        internal void SetParentContext(IListBlockContext<TItem, THeader> parentContext)
        {
            _parentContext = parentContext;
        }

        public string ListBlockId { get; set; }
        public int Attempt { get; set; }
        public THeader Header { get; set; }
        public IList<IListBlockItem<TItem>> Items
        {
            get
            {
                if (_items == null)
                {
                    _parentContext.GetItems();
                }

                return _items;
            }
            set
            {
                _items = value;
            }
        }
    }
}
