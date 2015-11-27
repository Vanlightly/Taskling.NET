using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;

namespace Taskling.InfrastructureContracts.Blocks.ListBlocks
{
    public class SingleUpdateRequest
    {
        public string ListBlockId { get; set; }
        public ListBlockItem ListBlockItem { get; set; }
    }
}
