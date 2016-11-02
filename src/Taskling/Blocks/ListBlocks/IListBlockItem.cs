using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks.ListBlocks
{
    public interface IListBlockItem<T>
    {
        string ListBlockItemId { get; }
        T Value { get; }
        ItemStatus Status { get; set; }
        string StatusReason { get; set; }
        DateTime LastUpdated { get; }
        byte? Step { get; set; }

        void Completed();
        void Failed(string message);
        void Discarded(string message);
    }
}
