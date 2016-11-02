using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.SqlServer.Tokens.Executions
{
    public class TokenResponse
    {
        public string ExecutionTokenId { get; set; }
        public DateTime StartedAt { get; set; }
        public GrantStatus GrantStatus { get; set; }
    }
}
