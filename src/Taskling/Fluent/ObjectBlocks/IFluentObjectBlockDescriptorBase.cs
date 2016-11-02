using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Fluent
{
    public interface IFluentObjectBlockDescriptorBase<T>
    {
        IOverrideConfigurationDescriptor WithObject(T data);
        IOverrideConfigurationDescriptor WithNoNewBlocks();
        IReprocessScopeDescriptor Reprocess();
    }
}
