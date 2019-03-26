using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Tests.Contexts
{
    public class TestHeader
    {
        public string PurchaseCode { get; set; }
        public DateTime FromDate { get; set; }
        public DateTime ToDate { get; set; }
    }
}
