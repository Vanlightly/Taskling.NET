using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks.ListBlocks
{
    public enum ItemStatus
    {
        NotDefined = 0,
        Pending = 1,
        Completed = 2,
        Failed = 3,
        Discarded = 4,
        All = 5
    }
}
