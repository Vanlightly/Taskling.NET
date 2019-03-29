using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Configuration;

namespace TasklingTesterAsync
{
    public class MyConfigReader : IConfigurationReader
    {
        private Dictionary<string, string> _config;

        public MyConfigReader()
        {
            _config = new Dictionary<string, string>();
            _config.Add("MyApplication::MyDateBasedBatchJob", "DB[Server=(local);Database=MyAppDb;Trusted_Connection=True;] TO[120] E[true] CON[1] KPLT[2] KPDT[40] MCI[1] KA[true] KAINT[1] KADT[10] TPDT[0] RPC_FAIL[true] RPC_FAIL_MTS[600] RPC_FAIL_RTYL[3] RPC_DEAD[true] RPC_DEAD_MTS[600] RPC_DEAD_RTYL[3] MXBL[2000]");
            _config.Add("MyApplication::MyNumericBasedBatchJob", "DB[Server=(local);Database=MyAppDb;Trusted_Connection=True;] TO[120] E[true] CON[1] KPLT[2] KPDT[40] MCI[1] KA[true] KAINT[1] KADT[10] TPDT[0] RPC_FAIL[true] RPC_FAIL_MTS[600] RPC_FAIL_RTYL[3] RPC_DEAD[true] RPC_DEAD_MTS[600] RPC_DEAD_RTYL[3] MXBL[2000]");
            _config.Add("MyApplication::MyListBasedBatchJob", "DB[Server=(local);Database=MyAppDb;Trusted_Connection=True;] TO[120] E[true] CON[1] KPLT[2] KPDT[40] MCI[1] KA[true] KAINT[1] KADT[10] TPDT[0] RPC_FAIL[true] RPC_FAIL_MTS[600] RPC_FAIL_RTYL[3] RPC_DEAD[true] RPC_DEAD_MTS[600] RPC_DEAD_RTYL[3] MXBL[2000]");
        }

        public string GetTaskConfigurationString(string applicationName, string taskName)
        {
            var key = applicationName + "::" + taskName;
            return _config[key];
        }
    }
}
