using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;

namespace Taskling.Fluent.RangeBlocks
{
    public class FluentRangeBlockDescriptor : IFluentDateRangeBlockDescriptor, IFluentNumericRangeBlockDescriptor
    {
        public IReprocessScopeDescriptor ReprocessDateRange()
        {
            return new FluentBlockSettingsDescriptor(BlockType.DateRange);
        }

        public IReprocessScopeDescriptor ReprocessNumericRange()
        {
            return new FluentBlockSettingsDescriptor(BlockType.NumericRange);
        }

        public IOverrideConfigurationDescriptor WithRange(DateTime fromDate, DateTime toDate, TimeSpan maxBlockRange)
        {
            return new FluentBlockSettingsDescriptor(fromDate, toDate, maxBlockRange);
        }

        public IOverrideConfigurationDescriptor WithRange(long fromNumber, long toNumber, long maxBlockNumberRange)
        {
            return new FluentBlockSettingsDescriptor(fromNumber, toNumber, maxBlockNumberRange);
        }

        public IOverrideConfigurationDescriptor OnlyOldDateBlocks()
        {
            return new FluentBlockSettingsDescriptor(BlockType.DateRange);
        }

        public IOverrideConfigurationDescriptor OnlyOldNumericBlocks()
        {
            return new FluentBlockSettingsDescriptor(BlockType.NumericRange);
        }
    }
}
