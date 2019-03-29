using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TasklingTesterAsync
{
    public interface IMyApplicationConfiguration
    {
        DateTime FirstRunDate { get; }
    }
}
