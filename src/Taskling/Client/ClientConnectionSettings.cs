using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Client
{
    public class ClientConnectionSettings
    {
        public string ConnectionString { get; set; }
        public TimeSpan ConnectTimeout { get; set; }
        public TimeSpan QueryTimeout { get; set; }
    }
}
