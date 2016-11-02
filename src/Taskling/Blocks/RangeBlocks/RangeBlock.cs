using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;

namespace Taskling.Blocks.RangeBlocks
{
    public class RangeBlock : IDateRangeBlock, INumericRangeBlock
    {
        public RangeBlock(string rangeBlockId,
            int attempt,
            long rangeBegin,
            long rangeEnd,
            BlockType blockType)
        {
            RangeBlockId = rangeBlockId;
            Attempt = attempt;
            RangeBegin = rangeBegin;
            RangeEnd = rangeEnd;
            RangeType = blockType;
        }

        public string RangeBlockId { get; set; }
        public int Attempt { get; set; }

        public long StartNumber
        {
            get
            {
                return RangeBegin;
            }
        }

        public long EndNumber
        {
            get
            {
                return RangeEnd;
            }
        }

        public DateTime StartDate
        {
            get
            {
                return RangeBeginAsDateTime();
            }
        }

        public DateTime EndDate
        {
            get
            {
                return RangeEndAsDateTime();
            }
        }

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
