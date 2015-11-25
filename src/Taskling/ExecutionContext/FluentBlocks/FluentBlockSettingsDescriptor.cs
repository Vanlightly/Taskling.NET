using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.ExecutionContext.FluentBlocks
{
    public class FluentBlockSettingsDescriptor : IFluentBlockSettingsDescriptor, IBlockSettings
    {
        public bool MustReprocessFailedTasks { get; set; }
        public TimeSpan FailedTaskDetectionRange { get; set; }

        public bool MustReprocessDeadTasks { get; set; }
        public TimeSpan DeadTaskDetectionRange { get; set; }
        public TimeSpan TreatAsDeadAfterRange { get; set; }

        public int MaximumNumberOfBlocksLimit { get; set; }

        public TaskDeathMode TaskDeathMode { get; set; }
        public RangeBlockType RangeType { get; set; }

        // Date Range
        public DateTime FromDate { get; set; }
        public DateTime ToDate { get; set; }
        public TimeSpan MaxBlockTimespan { get; set; }

        // Numeric Range
        public long FromNumber { get; set; }
        public long ToNumber { get; set; }
        public long MaxBlockNumberRange { get; set; }

        public IFluentBlockSettingsDescriptor ReprocessFailedTasks(TimeSpan detectionRange)
        {
            MustReprocessFailedTasks = true;
            FailedTaskDetectionRange = detectionRange;
            return this;
        }

        public IFluentBlockSettingsDescriptor ReprocessDeadTasks(TimeSpan detectionRange, TimeSpan treatAsDeadAfter)
        {
            MustReprocessDeadTasks = true;
            DeadTaskDetectionRange = detectionRange;
            TreatAsDeadAfterRange = treatAsDeadAfter;
            return this;
        }

        public IFluentBlockSettingsDescriptor MaximumBlocksToGenerate(int maximumNumberOfBlocks)
        {
            MaximumNumberOfBlocksLimit = maximumNumberOfBlocks;
            return this;
        }
    }
}
