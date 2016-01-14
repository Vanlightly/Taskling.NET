using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks
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
        
        public BlockType RangeType { get; set; }

        public bool IsEmpty()
        {
            return RangeBlockId == "0" && RangeBegin == 0 && RangeEnd == 0;
        }

        public int RangeBeginAsInt()
        {
            return (int)RangeBegin;
        }

        public int RangeBeginAsInt(int defaultIfEmptyValue)
        {
            if (IsEmpty())
                return defaultIfEmptyValue;
            return (int)RangeBegin;
        }

        public long RangeBeginAsLong()
        {
            return RangeBegin;
        }

        public long RangeBeginAsLong(long defaultIfEmptyValue)
        {
            if (IsEmpty())
                return defaultIfEmptyValue;

            return RangeBegin;
        }

        public DateTime RangeBeginAsDateTime()
        {
            return new DateTime(RangeBegin);
        }

        public DateTime RangeBeginAsDateTime(DateTime defaultIfEmptyValue)
        {
            if (IsEmpty())
                return defaultIfEmptyValue;

            return new DateTime(RangeBegin);
        }

        public int RangeEndAsInt()
        {
            return (int)RangeEnd;
        }

        public int RangeEndAsInt(int defaultIfEmptyValue)
        {
            if (IsEmpty())
                return defaultIfEmptyValue;

            return (int)RangeEnd;
        }

        public long RangeEndAsLong()
        {
            return RangeEnd;
        }

        public long RangeEndAsLong(long defaultIfEmptyValue)
        {
            if (IsEmpty())
                return defaultIfEmptyValue;

            return RangeEnd;
        }

        public DateTime RangeEndAsDateTime()
        {
            return new DateTime(RangeEnd);
        }

        public DateTime RangeEndAsDateTime(DateTime defaultIfEmptyValue)
        {
            if (IsEmpty())
                return defaultIfEmptyValue;

            return new DateTime(RangeEnd);
        }
        
        

    }
}
