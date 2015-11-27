using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Taskling.Blocks
{
    public interface IListBlockContext : IBlockContext
    {
        string ListBlockId { get; }
        IEnumerable<ListBlockItem> GetItems();
        void ItemComplete(ListBlockItem item);
        void ItemFailed(ListBlockItem item);
    }
}
