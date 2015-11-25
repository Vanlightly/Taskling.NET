using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Exceptions
{
    public class TransientException : Exception
    {
        public TransientException(string message)
            : base(message)
        {
        }

        public TransientException(string message, Exception innerException)
            : base(message, innerException)
        {
        }


    }
}
