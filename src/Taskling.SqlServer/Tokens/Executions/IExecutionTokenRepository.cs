using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Tokens.Executions
{
    public interface IExecutionTokenRepository
    {
        TokenResponse TryAcquireExecutionToken(TokenRequest tokenRequest);
        void ReturnExecutionToken(TokenRequest tokenRequest, string executionTokenId);
    }
}
