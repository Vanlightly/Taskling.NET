using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Configuration
{
    public interface ITasklingConfiguration
    {
        TaskConfiguration GetTaskConfiguration(string applicationName, string taskName);
    }
}
