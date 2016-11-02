using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Serialization;

namespace Taskling.Fluent.ListBlocks
{
    public class FluentListBlockDescriptorBase<T> : IFluentListBlockDescriptorBase<T>
    {
        public IOverrideConfigurationDescriptor WithSingleUnitCommit(List<T> values, short maxBlockSize)
        {
            var jsonValues = Serialize(values);
            var listBlockDescriptor = new FluentBlockSettingsDescriptor(jsonValues, maxBlockSize);
            listBlockDescriptor.ListUpdateMode = ListUpdateMode.SingleItemCommit;

            return listBlockDescriptor;
        }

        public IOverrideConfigurationDescriptor WithPeriodicCommit(List<T> values, short maxBlockSize, BatchSize batchSize)
        {
            var jsonValues = Serialize(values);
            var listBlockDescriptor = new FluentBlockSettingsDescriptor(jsonValues, maxBlockSize);
            listBlockDescriptor.ListUpdateMode = ListUpdateMode.PeriodicBatchCommit;

            switch (batchSize)
            {
                case BatchSize.NotSet:
                    listBlockDescriptor.UncommittedItemsThreshold = 100;
                    break;
                case BatchSize.Ten:
                    listBlockDescriptor.UncommittedItemsThreshold = 10;
                    break;
                case BatchSize.Fifty:
                    listBlockDescriptor.UncommittedItemsThreshold = 50;
                    break;
                case BatchSize.Hundred:
                    listBlockDescriptor.UncommittedItemsThreshold = 100;
                    break;
                case BatchSize.FiveHundred:
                    listBlockDescriptor.UncommittedItemsThreshold = 500;
                    break;
            }

            return listBlockDescriptor;
        }

        public IOverrideConfigurationDescriptor WithBatchCommitAtEnd(List<T> values, short maxBlockSize)
        {
            var jsonValues = Serialize(values);
            var listBlockDescriptor = new FluentBlockSettingsDescriptor(jsonValues, maxBlockSize);
            listBlockDescriptor.ListUpdateMode = ListUpdateMode.BatchCommitAtEnd;

            return listBlockDescriptor;
        }

        public IReprocessScopeDescriptor ReprocessWithSingleUnitCommit()
        {
            var listBlockDescriptor = new FluentBlockSettingsDescriptor(BlockType.List);
            listBlockDescriptor.ListUpdateMode = ListUpdateMode.SingleItemCommit;

            return listBlockDescriptor;
        }

        public IReprocessScopeDescriptor ReprocessWithPeriodicCommit(BatchSize batchSize)
        {
            var listBlockDescriptor = new FluentBlockSettingsDescriptor(BlockType.List);
            listBlockDescriptor.ListUpdateMode = ListUpdateMode.PeriodicBatchCommit;

            return listBlockDescriptor;
        }

        public IReprocessScopeDescriptor ReprocessWithBatchCommitAtEnd()
        {
            var listBlockDescriptor = new FluentBlockSettingsDescriptor(BlockType.List);
            listBlockDescriptor.ListUpdateMode = ListUpdateMode.BatchCommitAtEnd;

            return listBlockDescriptor;
        }

        private List<string> Serialize(List<T> values)
        {
            var jsonValues = new List<string>();
            foreach (var value in values)
            {
                jsonValues.Add(JsonGenericSerializer.Serialize<T>(value));
            }

            return jsonValues;
        }
    }

    public class FluentListBlockDescriptorBase<TItem, THeader> : IFluentListBlockDescriptorBase<TItem, THeader>
    {
        public IOverrideConfigurationDescriptor WithSingleUnitCommit(List<TItem> values, THeader header, short maxBlockSize)
        {
            var jsonValues = Serialize(values);
            var jsonHeader = Serialize(header);

            var listBlockDescriptor = new FluentBlockSettingsDescriptor(jsonValues, jsonHeader, maxBlockSize);
            listBlockDescriptor.ListUpdateMode = ListUpdateMode.SingleItemCommit;

            return listBlockDescriptor;
        }

        public IOverrideConfigurationDescriptor WithPeriodicCommit(List<TItem> values, THeader header, short maxBlockSize, BatchSize batchSize)
        {
            var jsonValues = Serialize(values);
            var jsonHeader = Serialize(header);
            var listBlockDescriptor = new FluentBlockSettingsDescriptor(jsonValues, jsonHeader, maxBlockSize);
            listBlockDescriptor.ListUpdateMode = ListUpdateMode.PeriodicBatchCommit;

            switch (batchSize)
            {
                case BatchSize.NotSet:
                    listBlockDescriptor.UncommittedItemsThreshold = 100;
                    break;
                case BatchSize.Ten:
                    listBlockDescriptor.UncommittedItemsThreshold = 10;
                    break;
                case BatchSize.Fifty:
                    listBlockDescriptor.UncommittedItemsThreshold = 50;
                    break;
                case BatchSize.Hundred:
                    listBlockDescriptor.UncommittedItemsThreshold = 100;
                    break;
                case BatchSize.FiveHundred:
                    listBlockDescriptor.UncommittedItemsThreshold = 500;
                    break;
            }

            return listBlockDescriptor;
        }

        public IOverrideConfigurationDescriptor WithBatchCommitAtEnd(List<TItem> values, THeader header, short maxBlockSize)
        {
            var jsonValues = Serialize(values);
            var jsonHeader = Serialize(header);
            var listBlockDescriptor = new FluentBlockSettingsDescriptor(jsonValues, jsonHeader, maxBlockSize);
            listBlockDescriptor.ListUpdateMode = ListUpdateMode.BatchCommitAtEnd;

            return listBlockDescriptor;
        }

        public IReprocessScopeDescriptor ReprocessWithSingleUnitCommit()
        {
            var listBlockDescriptor = new FluentBlockSettingsDescriptor(BlockType.List);
            listBlockDescriptor.ListUpdateMode = ListUpdateMode.SingleItemCommit;

            return listBlockDescriptor;
        }

        public IReprocessScopeDescriptor ReprocessWithPeriodicCommit(BatchSize batchSize)
        {
            var listBlockDescriptor = new FluentBlockSettingsDescriptor(BlockType.List);
            listBlockDescriptor.ListUpdateMode = ListUpdateMode.PeriodicBatchCommit;

            return listBlockDescriptor;
        }

        public IReprocessScopeDescriptor ReprocessWithBatchCommitAtEnd()
        {
            var listBlockDescriptor = new FluentBlockSettingsDescriptor(BlockType.List);
            listBlockDescriptor.ListUpdateMode = ListUpdateMode.BatchCommitAtEnd;

            return listBlockDescriptor;
        }

        private List<string> Serialize(List<TItem> values)
        {
            var jsonValues = new List<string>();
            foreach (var value in values)
            {
                jsonValues.Add(JsonGenericSerializer.Serialize<TItem>(value));
            }

            return jsonValues;
        }

        private string Serialize(THeader header)
        {
            return JsonGenericSerializer.Serialize<THeader>(header);
        }
    }
}
