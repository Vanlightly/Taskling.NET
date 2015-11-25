using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.ExecutionContext.FluentBlocks
{
    public class FluentBlockDescriptor
    {
        private FluentDateRangeBlockDescriptor _dateRangeBlockDescriptor;
        private FluentNumericRangeBlockDescriptor _numericRangeBlockDescriptor;

        public IFluentBlockSettingsDescriptor AsDateRange(DateTime fromDate, DateTime toDate, TimeSpan maxBlockRange)
        {
            _dateRangeBlockDescriptor = new FluentDateRangeBlockDescriptor(fromDate, toDate, maxBlockRange);

            return _dateRangeBlockDescriptor;
        }

        public IFluentBlockSettingsDescriptor AsNumericRange(long fromNumber, long toNumber, long maxBlockNumberRange)
        {
            _numericRangeBlockDescriptor = new FluentNumericRangeBlockDescriptor(fromNumber, toNumber, maxBlockNumberRange);

            return _numericRangeBlockDescriptor;
        }
    }
}
