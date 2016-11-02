using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Configuration
{
    [Serializable]
    public class TokenFormatException : Exception
    {
        public TokenFormatException(string message)
            : base(message)
        { }
    }
}
