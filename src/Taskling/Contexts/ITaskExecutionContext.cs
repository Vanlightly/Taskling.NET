using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.Fluent;
using Taskling.Fluent.RangeBlocks;
using Taskling.Tasks;

namespace Taskling.Contexts
{
    public interface ITaskExecutionContext : IDisposable
    {
        bool IsStarted { get; }
        bool TryStart();
        bool TryStart(string referenceValue);
        bool TryStart<TExecutionHeader>(TExecutionHeader executionHeader);
        bool TryStart<TExecutionHeader>(TExecutionHeader executionHeader, string referenceValue);
        void Complete();
        void Checkpoint(string checkpointMessage);
        void Error(string errorMessage, bool treatTaskAsFailed);
        TExecutionHeader GetHeader<TExecutionHeader>();
        ICriticalSectionContext CreateCriticalSection();
        IDateRangeBlock GetLastDateRangeBlock(LastBlockOrder lastBlockOrder);
        INumericRangeBlock GetLastNumericRangeBlock(LastBlockOrder lastBlockOrder);
        IListBlock<T> GetLastListBlock<T>();
        IListBlock<TItem, THeader> GetLastListBlock<TItem, THeader>();
        IObjectBlock<T> GetLastObjectBlock<T>();
        IList<IDateRangeBlockContext> GetDateRangeBlocks(Func<IFluentDateRangeBlockDescriptor, object> fluentBlockRequest);
        IList<INumericRangeBlockContext> GetNumericRangeBlocks(Func<IFluentNumericRangeBlockDescriptor, object> fluentBlockRequest);
        IList<IListBlockContext<T>> GetListBlocks<T>(Func<IFluentListBlockDescriptorBase<T>, object> fluentBlockRequest);
        IList<IListBlockContext<TItem, THeader>> GetListBlocks<TItem, THeader>(Func<IFluentListBlockDescriptorBase<TItem, THeader>, object> fluentBlockRequest);
        IList<IObjectBlockContext<T>> GetObjectBlocks<T>(Func<IFluentObjectBlockDescriptorBase<T>, object> fluentBlockRequest);
        TaskExecutionMeta GetLastExecutionMeta();
        IList<TaskExecutionMeta> GetLastExecutionMetas(int numberToRetrieve);
        TaskExecutionMeta<TExecutionHeader> GetLastExecutionMeta<TExecutionHeader>();
        IList<TaskExecutionMeta<TExecutionHeader>> GetLastExecutionMetas<TExecutionHeader>(int numberToRetrieve);
    }
}
