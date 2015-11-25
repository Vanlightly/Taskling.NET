using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.Blocks.Requests
{
    public class DateRangeBlockRequest : BlockRequest
    {
        public DateRangeBlockRequest()
        {
            RangeType = RangeBlockType.DateRange;
        }

        public DateTime RangeBegin { get; set; }
        public DateTime RangeEnd { get; set; }
        public TimeSpan MaxBlockRange { get; set; }
    }
}
