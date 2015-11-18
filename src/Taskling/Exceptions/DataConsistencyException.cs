using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Exceptions
{
    public class DataConsistencyException : Exception
    {
        public DataConsistencyException(string message)
            : base(message)
        {
        }

        public DataConsistencyException(string message, Exception innerException)
            : base(message, innerException)
        {
        }


    }
}
