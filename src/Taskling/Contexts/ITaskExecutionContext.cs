using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
        Task<bool> TryStartAsync();
        Task<bool> TryStartAsync(string referenceValue);
        Task<bool> TryStartAsync<TExecutionHeader>(TExecutionHeader executionHeader);
        Task<bool> TryStartAsync<TExecutionHeader>(TExecutionHeader executionHeader, string referenceValue);
        Task CompleteAsync();
        Task CheckpointAsync(string checkpointMessage);
        Task ErrorAsync(string errorMessage, bool treatTaskAsFailed);
        TExecutionHeader GetHeader<TExecutionHeader>();
        ICriticalSectionContext CreateCriticalSection();
        Task<IDateRangeBlock> GetLastDateRangeBlockAsync(LastBlockOrder lastBlockOrder);
        Task<INumericRangeBlock> GetLastNumericRangeBlockAsync(LastBlockOrder lastBlockOrder);
        Task<IListBlock<T>> GetLastListBlockAsync<T>();
        Task<IListBlock<TItem, THeader>> GetLastListBlockAsync<TItem, THeader>();
        Task<IObjectBlock<T>> GetLastObjectBlockAsync<T>();
        Task<IList<IDateRangeBlockContext>> GetDateRangeBlocksAsync(Func<IFluentDateRangeBlockDescriptor, object> fluentBlockRequest);
        Task<IList<INumericRangeBlockContext>> GetNumericRangeBlocksAsync(Func<IFluentNumericRangeBlockDescriptor, object> fluentBlockRequest);
        Task<IList<IListBlockContext<T>>> GetListBlocksAsync<T>(Func<IFluentListBlockDescriptorBase<T>, object> fluentBlockRequest);
        Task<IList<IListBlockContext<TItem, THeader>>> GetListBlocksAsync<TItem, THeader>(Func<IFluentListBlockDescriptorBase<TItem, THeader>, object> fluentBlockRequest);
        Task<IList<IObjectBlockContext<T>>> GetObjectBlocksAsync<T>(Func<IFluentObjectBlockDescriptorBase<T>, object> fluentBlockRequest);
        Task<TaskExecutionMeta> GetLastExecutionMetaAsync();
        Task<IList<TaskExecutionMeta>> GetLastExecutionMetasAsync(int numberToRetrieve);
        Task<TaskExecutionMeta<TExecutionHeader>> GetLastExecutionMetaAsync<TExecutionHeader>();
        Task<IList<TaskExecutionMeta<TExecutionHeader>>> GetLastExecutionMetasAsync<TExecutionHeader>(int numberToRetrieve);
    }
}
