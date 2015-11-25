//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using Taskling.InfrastructureContracts.TaskExecution;

//namespace Taskling.InfrastructureContracts.Blocks.DateRangeBlocks
//{
//    public class DateRangeBlockCreateRequest : BlockRequestBase
//    {
//        public DateRangeBlockCreateRequest(string applicationName, 
//            string taskName, 
//            int taskExecutionId,
//            DateTime fromDate,
//            DateTime toDate)
//            : base(applicationName, taskName, taskExecutionId)
//        {
//            FromDate = fromDate;
//            ToDate = toDate;
//        }

//        public DateTime FromDate { get; set; }
//        public DateTime ToDate { get; set; }
//    }
//}
