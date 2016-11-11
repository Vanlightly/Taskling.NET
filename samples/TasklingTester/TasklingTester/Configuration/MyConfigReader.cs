using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Configuration;

namespace TasklingTester
{
    public class MyConfigReader : IConfigurationReader
    {
        public string GetTaskConfigurationString(string applicationName, string taskName)
        {
            var key = applicationName + "::" + taskName;
            var configString = ConfigurationManager.AppSettings.Get(key);

            return configString;
        }
    }
}
