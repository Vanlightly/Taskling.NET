using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.ListBlocks;

namespace Taskling.InfrastructureContracts.Blocks.ListBlocks
{
    public class ProtoListBlockItem
    {
        public string ListBlockItemId { get; set; }
        public string Value { get; set; }
        public ItemStatus Status { get; set; }
        public string StatusReason { get; set; }
        public DateTime LastUpdated { get; set; }
        public byte? Step { get; set; }
    }
}
