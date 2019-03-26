using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Tests.Contexts.Given_ObjectBlockContext
{
    public class MyComplexClass
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime DateOfBirth { get; set; }
        public MyOtherComplexClass SomeOtherData { get; set; }
    }

    public class MyOtherComplexClass
    {
        public decimal Value { get; set; }
        public List<string> Notes { get; set; }
    }
}
