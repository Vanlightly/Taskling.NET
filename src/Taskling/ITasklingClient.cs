using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Contexts;

namespace Taskling
{
    public interface ITasklingClient
    {
        ITaskExecutionContext CreateTaskExecutionContext(string applicationName, string taskName);
    }
}
