using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Requests;

namespace Taskling.Blocks
{
    public interface IBlockFactory
    {
        IList<IRangeBlockContext> GenerateDateRangeBlocks(DateRangeBlockRequest blockRequest);
        IList<IRangeBlockContext> GenerateNumericRangeBlocks(NumericRangeBlockRequest blockRequest);
        IList<IListBlockContext> GenerateListBlocks(ListBlockRequest blockRequest);
    }
}
