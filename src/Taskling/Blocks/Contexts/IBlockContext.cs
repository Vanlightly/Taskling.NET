using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks
{
    public interface IBlockContext
    {
        void Start();
        void Complete();
        void Failed();
    }
}
