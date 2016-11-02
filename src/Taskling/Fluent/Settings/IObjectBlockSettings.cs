using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Fluent.Settings
{
    public interface IObjectBlockSettings<T> : IBlockSettings
    {
        T Object { get; set; }
    }
}
