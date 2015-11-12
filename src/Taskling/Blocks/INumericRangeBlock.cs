using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks
{
    public interface INumericRangeBlock : IBlock
    {
        long FromId { get; set; }
        long ToId { get; set; }
        long Interval { get; set; }
    }
}
