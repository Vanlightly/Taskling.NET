using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Configuration
{
    public interface IConfigurationReader
    {
        string GetTaskConfigurationString(string applicationName, string taskName);
    }
}
