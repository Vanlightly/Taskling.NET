using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Tasks;

namespace Taskling.Fluent.Settings
{
    public interface IBlockSettings
    {
        BlockType BlockType { get; set; }

        // DateRange
        DateTime? FromDate { get; set; }
        DateTime? ToDate { get; set; }
        TimeSpan? MaxBlockTimespan { get; set; }

        // NumericRange
        long? FromNumber { get; set; }
        long? ToNumber { get; set; }
        long? MaxBlockNumberRange { get; set; }

        // ListBlocks
        List<string> Values { get; set; }
        string Header { get; set; }
        short MaxBlockSize { get; set; }
        ListUpdateMode ListUpdateMode { get; set; }
        int UncommittedItemsThreshold { get; set; }

        // Reprocess Specific Task
        ReprocessOption ReprocessOption { get; set; }
        string ReferenceValueToReprocess { get; set; }

        // Configuration Overridable
        bool? MustReprocessFailedTasks { get; set; }
        TimeSpan? FailedTaskDetectionRange { get; set; }
        short? FailedTaskRetryLimit { get; set; }

        bool? MustReprocessDeadTasks { get; set; }
        TimeSpan? DeadTaskDetectionRange { get; set; }
        short? DeadTaskRetryLimit { get; set; }

        int? MaximumNumberOfBlocksLimit { get; set; }
    }
}
