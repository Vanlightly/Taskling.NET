using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks
{
    public class ForcedListBlockQueueItem : ForcedBlockQueueItem
    {
        public ProtoListBlock ListBlock { get; set; }
    }
}
