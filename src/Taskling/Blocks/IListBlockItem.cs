using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks
{
    public interface IListBlockItem
    {
        void Start();
        void Complete();
        void Failed(string failureMessage);
    }
}
