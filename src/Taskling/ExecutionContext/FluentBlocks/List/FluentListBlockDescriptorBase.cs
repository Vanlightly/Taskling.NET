using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;

namespace Taskling.ExecutionContext.FluentBlocks.List
{
    public class FluentListBlockDescriptorBase
    {
        private FluentListBlockDescriptor _listBlockDescriptor;

        public IFluentBlockSettingsDescriptor WithSingleUnitCommit(List<string> values, short maxBlockSize)
        {
            _listBlockDescriptor = new FluentListBlockDescriptor(values, maxBlockSize);
            _listBlockDescriptor.ListUpdateMode = ListUpdateMode.SingleItemCommit;

            return _listBlockDescriptor;
        }

        public IFluentBlockSettingsDescriptor WithPeriodicCommit(List<string> values, short maxBlockSize, BatchSize batchSize)
        {
            _listBlockDescriptor = new FluentListBlockDescriptor(values, maxBlockSize);
            _listBlockDescriptor.ListUpdateMode = ListUpdateMode.PeriodicBatchCommit;

            switch (batchSize)
            {
                case BatchSize.NotSet:
                    _listBlockDescriptor.UncommittedItemsThreshold = 100;
                    break;
                case BatchSize.Ten:
                    _listBlockDescriptor.UncommittedItemsThreshold = 10;
                    break;
                case BatchSize.Fifty:
                    _listBlockDescriptor.UncommittedItemsThreshold = 50;
                    break;
                case BatchSize.Hundred:
                    _listBlockDescriptor.UncommittedItemsThreshold = 100;
                    break;
                case BatchSize.FiveHundred:
                    _listBlockDescriptor.UncommittedItemsThreshold = 500;
                    break;
            }

            return _listBlockDescriptor;
        }

        public IFluentBlockSettingsDescriptor WithBatchCommitAtEnd(List<string> values, short maxBlockSize)
        {
            _listBlockDescriptor = new FluentListBlockDescriptor(values, maxBlockSize);
            _listBlockDescriptor.ListUpdateMode = ListUpdateMode.BatchCommitAtEnd;

            return _listBlockDescriptor;
        }
    }
}
