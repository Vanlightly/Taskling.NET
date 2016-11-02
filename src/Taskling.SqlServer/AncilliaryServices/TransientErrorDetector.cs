using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.AncilliaryServices
{
    public class TransientErrorDetector
    {
        public static bool IsTransient(SqlException sqlEx)
        {
            if (sqlEx.Number == 1205 // 1205 = Deadlock
                || sqlEx.Number == -2 // -2 = TimeOut
                || sqlEx.Number == -1 // -1 = Connection
                || sqlEx.Number == 2 // 2 = Connection
                || sqlEx.Number == 53 // 53 = Connection
                )
                return true;

            return false;
        }
    }
}
