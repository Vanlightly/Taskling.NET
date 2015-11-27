using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.ExecutionContext.FluentBlocks
{
    public class FluentDateRangeBlockDescriptor : FluentBlockSettingsDescriptor
    {
        public FluentDateRangeBlockDescriptor(DateTime fromDate, DateTime toDate, TimeSpan maxBlockRange)
        {
            FromDate = fromDate;
            ToDate = toDate;
            MaxBlockTimespan = maxBlockRange;
            BlockType = BlockType.DateRange;
        }
    }
}
