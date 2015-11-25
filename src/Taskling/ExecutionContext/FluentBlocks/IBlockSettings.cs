using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.ExecutionContext.FluentBlocks
{
    public interface IBlockSettings
    {
        RangeBlockType RangeType { get; set; }

        bool MustReprocessFailedTasks { get; set; }
        TimeSpan FailedTaskDetectionRange { get; set; }

        bool MustReprocessDeadTasks { get; set; }
        TimeSpan DeadTaskDetectionRange { get; set; }
        TimeSpan TreatAsDeadAfterRange { get; set; }

        int MaximumNumberOfBlocksLimit { get; set; }

        // DateRange
        DateTime FromDate { get; set; }
        DateTime ToDate { get; set; }
        TimeSpan MaxBlockTimespan { get; set; }

        // NumericRange
        long FromNumber { get; set; }
        long ToNumber { get; set; }
        long MaxBlockNumberRange { get; set; }
    }
}
