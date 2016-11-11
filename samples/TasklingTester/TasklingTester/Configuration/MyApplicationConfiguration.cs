using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TasklingTester.Configuration
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
