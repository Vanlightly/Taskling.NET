using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        Task CompletedAsync();
        Task FailedAsync(string message);
        Task DiscardedAsync(string message);
    }
}
