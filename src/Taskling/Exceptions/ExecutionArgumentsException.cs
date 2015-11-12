using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Exceptions
{
    public class ExecutionArgumentsException : Exception
    {
        public ExecutionArgumentsException(string message)
            : base(message)
        {
        }

        public ExecutionArgumentsException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        
    }
}
