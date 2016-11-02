using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.RangeBlocks;

namespace Taskling.Contexts
{
    public interface IDateRangeBlockContext : IBlockContext
    {
        IDateRangeBlock DateRangeBlock { get; }
        string ForcedBlockQueueId { get; }
        void Complete(int itemsProcessed);
    }
}
