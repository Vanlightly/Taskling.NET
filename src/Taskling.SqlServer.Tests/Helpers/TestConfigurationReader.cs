using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Configuration;

namespace Taskling.SqlServer.Tests.Helpers
{
    public class TestConfigurationReader : IConfigurationReader
    {
        private string _configString;

        public TestConfigurationReader(string configString)
        {
            _configString = configString;
        }

        public string GetTaskConfigurationString(string applicationName, string taskName)
        {
            return _configString; // "DB(Server=(local);Database=TasklingDb;Trusted_Connection=True;) TO(120) E(true) CON(-1) KPLT(2) KPDT(40) MCI(1) KA(true) KAINT(1) KADT(10) TPDT(0) RPC_FAIL(true) RPC_FAIL_MTS(600) RPC_FAIL_RTYL(3) RPC_DEAD(true) RPC_DEAD_MTS(600) RPC_DEAD_RTYL(3) MXBL(20)";
        }
    }
}
