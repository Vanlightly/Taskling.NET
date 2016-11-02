using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Fluent.Settings;

namespace Taskling.Fluent.ObjectBlocks
{
    public class FluentObjectBlockSettings<T> : FluentBlockSettingsDescriptor, IObjectBlockSettings<T>
    {
        public FluentObjectBlockSettings()
            : base(BlockType.Object)
        { }

        public FluentObjectBlockSettings(T objectData)
            : base(BlockType.Object)
        {
            Object = objectData;
        }

        public T Object { get; set; }
    }
}
