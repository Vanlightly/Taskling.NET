using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks.Requests
{
    public class DateRangeBlockRequest : BlockRequest
    {
        public DateRangeBlockRequest()
        {
            BlockType = Taskling.Blocks.Common.BlockType.DateRange;
        }

        public DateTime? RangeBegin { get; set; }
        public DateTime? RangeEnd { get; set; }
        public TimeSpan? MaxBlockRange { get; set; }
    }
}
