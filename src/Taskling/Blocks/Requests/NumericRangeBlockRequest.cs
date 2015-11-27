using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.Blocks.Requests
{
    public class NumericRangeBlockRequest : BlockRequest
    {
        public NumericRangeBlockRequest()
        {
            BlockType = BlockType.NumericRange;
        }

        public long RangeBegin { get; set; }
        public long RangeEnd { get; set; }
        public long BlockSize { get; set; }
    }
}
