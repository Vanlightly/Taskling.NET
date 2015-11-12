using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Exceptions
{
    public class ExecutionException : Exception
    {
        public ExecutionException(string message)
            : base(message)
        {
        }

        public ExecutionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        
    }
}
