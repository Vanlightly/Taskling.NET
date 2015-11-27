using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.Blocks.Requests;
using Taskling.CriticalSection;
using Taskling.ExecutionContext.FluentBlocks;
using Taskling.ExecutionContext.FluentBlocks.List;

namespace Taskling.ExecutionContext
{
    public interface ITaskExecutionContext : IDisposable
    {
        bool TryStart();
        void Complete();
        void Checkpoint(string checkpointMessage);
        void Error(string errorMessage);
        ICriticalSectionContext CreateCriticalSection();
        IList<IRangeBlockContext> GetRangeBlocks(Func<FluentRangeBlockDescriptor, IFluentBlockSettingsDescriptor> fluentBlockRequest);
        IList<IListBlockContext> GetListBlocks(Func<FluentListBlockDescriptorBase, IFluentBlockSettingsDescriptor> fluentBlockRequest);
        
    }
}
