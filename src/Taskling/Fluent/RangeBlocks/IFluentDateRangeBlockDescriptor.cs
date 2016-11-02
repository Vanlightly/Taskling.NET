using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Fluent
{
    public interface IFluentDateRangeBlockDescriptor
    {
        IOverrideConfigurationDescriptor WithRange(DateTime fromDate, DateTime toDate, TimeSpan maxBlockRange);
        IOverrideConfigurationDescriptor OnlyOldDateBlocks();
        IReprocessScopeDescriptor ReprocessDateRange();
    }
}
