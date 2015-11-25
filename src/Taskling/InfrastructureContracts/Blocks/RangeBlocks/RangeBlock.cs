using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.Blocks.RangeBlocks
{
    public class RangeBlock
    {
        public RangeBlock(string rangeBlockId,
            long rangeBegin,
            long rangeEnd)
        {
            RangeBlockId = rangeBlockId;
            RangeBegin = rangeBegin;
            RangeEnd = rangeEnd;
        }

        public string RangeBlockId { get; set; }
        private long RangeBegin { get; set; }
        private long RangeEnd { get; set; }
        
        public RangeBlockType RangeType { get; set; }

        public int RangeBeginAsInt()
        {
            return (int)RangeBegin;
        }

        public long RangeBeginAsLong()
        {
            return RangeBegin;
        }

        public DateTime RangeBeginAsDateTime()
        {
            return new DateTime(RangeBegin);
        }

        public int RangeEndAsInt()
        {
            return (int)RangeEnd;
        }

        public long RangeEndAsLong()
        {
            return RangeEnd;
        }

        public DateTime RangeEndAsDateTime()
        {
            return new DateTime(RangeEnd);
        }
        
        

    }
}
