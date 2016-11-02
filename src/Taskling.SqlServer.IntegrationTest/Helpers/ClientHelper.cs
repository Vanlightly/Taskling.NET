using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Configuration;
using Taskling.Contexts;

namespace Taskling.SqlServer.IntegrationTest.Helpers
{
    public class ClientHelper
    {
        private const string ConfigString = "DB[Server=(local);Database=TasklingDb;Trusted_Connection=True;] TO[120] E[true] CON[{0}] KPLT[{1}] KPDT[{2}] MCI[{3}] KA[{4}] KAINT[{5}] KADT[{6}] TPDT[{7}] RPC_FAIL[{8}] RPC_FAIL_MTS[{9}] RPC_FAIL_RTYL[{10}] RPC_DEAD[{11}] RPC_DEAD_MTS[{12}] RPC_DEAD_RTYL[{13}] MXBL[{14}]";

        public static ITaskExecutionContext GetExecutionContext(string taskName, string configString)
        {
            var client = CreateClient(configString);
            return client.CreateTaskExecutionContext(TestConstants.ApplicationName, taskName);
        }

        private static TasklingClient CreateClient(string configString)
        {
            return new TasklingClient(new TestConfigurationReader(configString));
        }

        public static string GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(int maxBlocksToGenerate = 2000)
        {
            return string.Format(ConfigString, "1", "2000", "2000", "1", "true", "1", "10", "0", "true", "600", "3", "true", "600", "3", maxBlocksToGenerate);
        }

        public static string GetDefaultTaskConfigurationWithKeepAliveAndNoReprocessing(int maxBlocksToGenerate = 2000)
        {
            return string.Format(ConfigString, "1", "2000", "2000", "1", "true", "1", "2", "0", "false", "0", "0", "false", "0", "0", maxBlocksToGenerate);
        }

        public static string GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing(int maxBlocksToGenerate = 2000)
        {
            return string.Format(ConfigString, "1", "2000", "2000", "1", "false", "0", "0", "240", "true", "600", "3", "true", "600", "3", maxBlocksToGenerate);
        }

        public static string GetDefaultTaskConfigurationWithTimePeriodOverrideAndNoReprocessing(int maxBlocksToGenerate = 2000)
        {
            return string.Format(ConfigString, "1", "2000", "2000", "1", "false", "0", "0", "240", "false", "0", "0", "false", "0", "0", maxBlocksToGenerate);
        }
    }
}
