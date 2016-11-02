using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Contexts
{
    public interface IBlockContext
    {
        void Start();
        void Complete();
        void Failed();
        void Failed(string message);
    }
}
