using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.ExecutionContext.FluentBlocks
{
    public class FluentNumericRangeBlockDescriptor : FluentBlockSettingsDescriptor
    {
        public FluentNumericRangeBlockDescriptor(long fromNumber, long toNumber, long maxBlockRange)
        {
            FromNumber = fromNumber;
            ToNumber = toNumber;
            MaxBlockNumberRange = maxBlockRange;
            RangeType = RangeBlockType.NumericRange;
        }
    }
}
