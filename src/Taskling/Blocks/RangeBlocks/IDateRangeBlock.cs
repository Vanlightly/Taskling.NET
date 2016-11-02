using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks.RangeBlocks
{
    public interface IDateRangeBlock
    {
        string RangeBlockId { get; }
        int Attempt { get; set; }
        DateTime StartDate { get; }
        DateTime EndDate { get; }
    }
}
