using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;

namespace Taskling.Client
{
    public interface ITasklingClient
    {
        ITaskExecutionContext CreateTaskExecutionContext(string applicationName, string taskName, TaskExecutionOptions taskExecutionOptions);
    }
}
