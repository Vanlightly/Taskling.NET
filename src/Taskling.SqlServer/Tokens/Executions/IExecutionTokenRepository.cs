using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Taskling.SqlServer.Tokens.Executions
{
    public interface IExecutionTokenRepository
    {
        Task<TokenResponse> TryAcquireExecutionTokenAsync(TokenRequest tokenRequest);
        Task ReturnExecutionTokenAsync(TokenRequest tokenRequest, string executionTokenId);
    }
}
