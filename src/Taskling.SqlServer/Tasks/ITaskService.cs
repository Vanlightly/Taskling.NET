using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.SqlServer.DataObjects;

namespace Taskling.SqlServer.Tasks
{
    public interface ITaskService
    {
        TaskDefinition GetTaskDefinition(string applicationName, string taskName);
    }
}
