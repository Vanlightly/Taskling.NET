using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Exceptions
{
    [Serializable]
    public class CouldNotStartException : Exception
    {
        public CouldNotStartException(string message)
            : base(message)
        {
        }

    }
}
