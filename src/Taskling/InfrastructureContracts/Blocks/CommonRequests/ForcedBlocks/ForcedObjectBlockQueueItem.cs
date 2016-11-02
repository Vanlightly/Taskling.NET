using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.ObjectBlocks;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks
{
    public class ForcedObjectBlockQueueItem<T> : ForcedBlockQueueItem
    {
        public ObjectBlock<T> ObjectBlock { get; set; }
    }
}
