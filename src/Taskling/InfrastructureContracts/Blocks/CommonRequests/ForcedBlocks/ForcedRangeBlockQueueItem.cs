using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.RangeBlocks;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks
{
    public class ForcedRangeBlockQueueItem : ForcedBlockQueueItem
    {
        public RangeBlock RangeBlock { get; set; }
    }
}
