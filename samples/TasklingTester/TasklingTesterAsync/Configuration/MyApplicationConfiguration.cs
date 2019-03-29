using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TasklingTesterAsync.Configuration
{
    public class MyApplicationConfiguration : IMyApplicationConfiguration
    {
        public DateTime FirstRunDate
        {
            get
            {
                return DateTime.Now.AddHours(-3);
            }
        }
    }
}
