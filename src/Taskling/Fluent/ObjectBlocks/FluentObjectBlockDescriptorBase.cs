using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Fluent.ObjectBlocks
{
    public class FluentObjectBlockDescriptorBase<T> : IFluentObjectBlockDescriptorBase<T>
    {
        public IOverrideConfigurationDescriptor WithObject(T data)
        {
            var stringBlockDescriptor = new FluentObjectBlockSettings<T>(data);

            return stringBlockDescriptor;
        }

        public IOverrideConfigurationDescriptor WithNoNewBlocks()
        {
            return new FluentObjectBlockSettings<T>();
        }

        public IReprocessScopeDescriptor Reprocess()
        {
            return new FluentObjectBlockSettings<T>();
        }
    }
}
