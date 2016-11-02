using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.ListBlocks;

namespace Taskling.Fluent
{
    public interface IFluentListBlockDescriptorBase<T>
    {
        IOverrideConfigurationDescriptor WithSingleUnitCommit(List<T> values, short maxBlockSize);
        IOverrideConfigurationDescriptor WithPeriodicCommit(List<T> values, short maxBlockSize, BatchSize batchSize);
        IOverrideConfigurationDescriptor WithBatchCommitAtEnd(List<T> values, short maxBlockSize);
        IReprocessScopeDescriptor ReprocessWithSingleUnitCommit();
        IReprocessScopeDescriptor ReprocessWithPeriodicCommit(BatchSize batchSize);
        IReprocessScopeDescriptor ReprocessWithBatchCommitAtEnd();
    }

    public interface IFluentListBlockDescriptorBase<TItem, THeader>
    {
        IOverrideConfigurationDescriptor WithSingleUnitCommit(List<TItem> values, THeader header, short maxBlockSize);
        IOverrideConfigurationDescriptor WithPeriodicCommit(List<TItem> values, THeader header, short maxBlockSize, BatchSize batchSize);
        IOverrideConfigurationDescriptor WithBatchCommitAtEnd(List<TItem> values, THeader header, short maxBlockSize);
        IReprocessScopeDescriptor ReprocessWithSingleUnitCommit();
        IReprocessScopeDescriptor ReprocessWithPeriodicCommit(BatchSize batchSize);
        IReprocessScopeDescriptor ReprocessWithBatchCommitAtEnd();
    }
}
