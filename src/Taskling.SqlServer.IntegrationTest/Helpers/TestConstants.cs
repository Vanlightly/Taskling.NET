using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.IntegrationTest.Helpers
{
    internal class TestConstants
    {
        internal const string TestConnectionString = "Server=(local);Database=TasklingDb;Trusted_Connection=True;";
        internal const string ApplicationName = "MyTestApplication";
        internal const string TaskName = "MyTestTask";
        internal static TimeSpan QueryTimeout = new TimeSpan(0, 1, 0);
    }
}
