using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Tokens.Executions
{
    public class ExecutionToken
    {
        public string TokenId { get; set; }
        public ExecutionTokenStatus Status { get; set; }
        public string GrantedToExecution { get; set; }
    }
}
