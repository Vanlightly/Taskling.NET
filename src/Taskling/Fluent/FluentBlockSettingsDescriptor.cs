using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Fluent.Settings;
using Taskling.Tasks;

namespace Taskling.Fluent
{
    public class FluentBlockSettingsDescriptor : IFluentBlockSettingsDescriptor, IOverrideConfigurationDescriptor, IReprocessScopeDescriptor, IReprocessTaskDescriptor, IBlockSettings, IComplete
    {
        public FluentBlockSettingsDescriptor(BlockType blockType)
        {
            BlockType = blockType;
        }

        public FluentBlockSettingsDescriptor(DateTime fromDate, DateTime toDate, TimeSpan maxBlockRange)
        {
            FromDate = fromDate;
            ToDate = toDate;
            MaxBlockTimespan = maxBlockRange;
            BlockType = BlockType.DateRange;
        }

        public FluentBlockSettingsDescriptor(long fromNumber, long toNumber, long maxBlockRange)
        {
            FromNumber = fromNumber;
            ToNumber = toNumber;
            MaxBlockNumberRange = maxBlockRange;
            BlockType = BlockType.NumericRange;
        }

        public FluentBlockSettingsDescriptor(List<string> values, short maxBlockSize)
        {
            Values = values;
            MaxBlockSize = maxBlockSize;
            BlockType = BlockType.List;
        }

        public FluentBlockSettingsDescriptor(List<string> values, string header, short maxBlockSize)
        {
            Values = values;
            Header = header;
            MaxBlockSize = maxBlockSize;
            BlockType = BlockType.List;
        }

        public bool? MustReprocessFailedTasks { get; set; }
        public TimeSpan? FailedTaskDetectionRange { get; set; }
        public short? FailedTaskRetryLimit { get; set; }

        public bool? MustReprocessDeadTasks { get; set; }
        public TimeSpan? DeadTaskDetectionRange { get; set; }
        public short? DeadTaskRetryLimit { get; set; }

        public int? MaximumNumberOfBlocksLimit { get; set; }

        public TaskDeathMode TaskDeathMode { get; set; }
        public BlockType BlockType { get; set; }

        // Date Range
        public DateTime? FromDate { get; set; }
        public DateTime? ToDate { get; set; }
        public TimeSpan? MaxBlockTimespan { get; set; }

        // Numeric Range
        public long? FromNumber { get; set; }
        public long? ToNumber { get; set; }
        public long? MaxBlockNumberRange { get; set; }

        // ListBlocks
        public List<string> Values { get; set; }
        public string Header { get; set; }
        public short MaxBlockSize { get; set; }
        public ListUpdateMode ListUpdateMode { get; set; }
        public int UncommittedItemsThreshold { get; set; }

        // Reprocess Specific Task
        public ReprocessOption ReprocessOption { get; set; }
        public string ReferenceValueToReprocess { get; set; }

        public IFluentBlockSettingsDescriptor ReprocessFailedTasks(TimeSpan detectionRange, short retryLimit)
        {
            MustReprocessFailedTasks = true;
            FailedTaskDetectionRange = detectionRange;
            FailedTaskRetryLimit = retryLimit;
            return this;
        }

        public IFluentBlockSettingsDescriptor ReprocessDeadTasks(TimeSpan detectionRange, short retryLimit)
        {
            MustReprocessDeadTasks = true;
            DeadTaskDetectionRange = detectionRange;
            DeadTaskRetryLimit = retryLimit;
            return this;
        }

        public IComplete MaximumBlocksToGenerate(int maximumNumberOfBlocks)
        {
            MaximumNumberOfBlocksLimit = maximumNumberOfBlocks;
            return this;
        }

        public IComplete OfExecutionWith(string referenceValue)
        {
            ReferenceValueToReprocess = referenceValue;
            return this;
        }

        public IReprocessTaskDescriptor AllBlocks()
        {
            ReprocessOption = ReprocessOption.Everything;
            return this;
        }

        public IReprocessTaskDescriptor PendingAndFailedBlocks()
        {
            ReprocessOption = ReprocessOption.PendingOrFailed;
            return this;
        }

        public IFluentBlockSettingsDescriptor OverrideConfiguration()
        {
            return this;
        }
    }
}
