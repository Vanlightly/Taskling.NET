using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks.RangeBlocks
{
    public interface INumericRangeBlock
    {
        string RangeBlockId { get; }
        int Attempt { get; set; }
        long StartNumber { get; }
        long EndNumber { get; }
    }
}
