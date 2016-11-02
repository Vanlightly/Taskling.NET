using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts;

namespace Taskling.SqlServer.Tokens.Executions
{
    public class TokenRequest
    {
        public TaskId TaskId { get; set; }
        public int TaskDefinitionId { get; set; }
        public string TaskExecutionId { get; set; }
        public int ConcurrencyLimit { get; set; }
    }
}
