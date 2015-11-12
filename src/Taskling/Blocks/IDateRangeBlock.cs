using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks
{
    public interface IDateRangeBlock : IBlock
    {
        DateTime FromDate { get; set; }
        DateTime ToDate { get; set; }
        TimeSpan Interval { get; set; }
    }
}
