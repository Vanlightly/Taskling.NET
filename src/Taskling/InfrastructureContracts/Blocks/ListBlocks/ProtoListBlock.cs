using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.Blocks.ListBlocks
{
    public class ProtoListBlock
    {
        public string ListBlockId { get; set; }
        public int Attempt { get; set; }
        public string Header { get; set; }
        public bool IsForcedBlock { get; set; }
        public int ForcedBlockQueueId { get; set; }
        public IList<ProtoListBlockItem> Items { get; set; }
    }
}
