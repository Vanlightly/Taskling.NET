using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Fluent.RangeBlocks
{
    public interface IFluentNumericRangeBlockDescriptor
    {
        IOverrideConfigurationDescriptor WithRange(long fromNumber, long toNumber, long maxBlockNumberRange);
        IOverrideConfigurationDescriptor OnlyOldNumericBlocks();
        IReprocessScopeDescriptor ReprocessNumericRange();
    }
}
