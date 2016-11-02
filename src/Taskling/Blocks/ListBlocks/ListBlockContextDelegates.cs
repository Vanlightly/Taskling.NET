using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks.ListBlocks
{
    public delegate void ItemCompleteDelegate<T>(IListBlockItem<T> item);
    public delegate void ItemFailedDelegate<T>(IListBlockItem<T> item, string reason, byte? step = null);
    public delegate void DiscardItemDelegate<T>(IListBlockItem<T> item, string reason, byte? step = null);
}
