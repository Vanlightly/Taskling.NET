using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Tasks
{
    public class ClientConnectionSettings
    {
        public ClientConnectionSettings(string connectionString, TimeSpan queryTimeout)
        {
            ConnectionString = connectionString;
            QueryTimeout = queryTimeout;
        }

        public string ConnectionString { get; set; }
        public TimeSpan QueryTimeout { get; set; }

        public int QueryTimeoutSeconds
        {
            get
            {
                return (int)QueryTimeout.TotalSeconds;
            }
        }

    }
}
